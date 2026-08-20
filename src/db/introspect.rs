//! Read-only catalog lookups shared by apply-time drop generation and
//! plan-time severity classification.

use tokio_postgres::GenericClient;
use crate::sql::ObjectType;

pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace("\"", "\"\""))
}

pub fn quote_qualified_identifier(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_identifier(s), quote_identifier(name)),
        None => quote_identifier(name),
    }
}

pub async fn get_existing_function_signatures<C: GenericClient>(
    client: &C,
    object_type: &ObjectType,
    qualified_name: &crate::sql::QualifiedIdent,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let (schema_name, function_name) = match &qualified_name.schema {
        Some(s) => (s.as_str(), qualified_name.name.as_str()),
        None => ("public", qualified_name.name.as_str()),
    };

    // Handle operators separately as they use pg_operator, not pg_proc
    if object_type == &ObjectType::Operator {
        let query = r#"
            SELECT
                CASE
                    WHEN n.nspname = 'public' THEN o.oprname
                    ELSE n.nspname || '.' || o.oprname
                END || '(' ||
                COALESCE(tl.typname, 'NONE') || ', ' ||
                COALESCE(tr.typname, 'NONE') || ')' AS signature
            FROM pg_operator o
            JOIN pg_namespace n ON n.oid = o.oprnamespace
            LEFT JOIN pg_type tl ON tl.oid = o.oprleft
            LEFT JOIN pg_type tr ON tr.oid = o.oprright
            WHERE n.nspname = $1
              AND o.oprname = $2
        "#;

        let rows = client.query(query, &[&schema_name, &function_name]).await?;

        let signatures: Vec<String> = rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect();

        return Ok(signatures);
    }

    let prokind: &str = match object_type {
        ObjectType::Function => "f",
        ObjectType::Procedure => "p",
        ObjectType::Aggregate => "a",
        _ => return Ok(vec![]),
    };

    // Query to get all overloads of a function with their full signatures
    let query = r#"
        SELECT
            CASE
                WHEN n.nspname = 'public' THEN p.proname
                ELSE n.nspname || '.' || p.proname
            END || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS signature
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1
          AND p.proname = $2
          AND p.prokind = $3::char
    "#;

    let rows = client.query(query, &[&schema_name, &function_name, &prokind]).await?;

    let signatures: Vec<String> = rows.iter()
        .map(|row| row.get::<_, String>(0))
        .collect();

    Ok(signatures)
}

/// The parts of a pg_proc row needed to compare a routine's interface
/// (identity arguments, parameter names, and result shape).
#[derive(Debug, Clone)]
pub struct RoutineCatalogInfo {
    pub oid: u32,
    pub rettype: u32,
    pub retset: bool,
    /// Types of ALL parameters (in, out, inout, variadic, table); NULL in the
    /// catalog — and None here — unless the routine has non-IN parameters.
    pub allargtypes: Option<Vec<u32>>,
    /// Modes aligned with `allargtypes`: "i", "o", "b", "v", "t".
    pub argmodes: Option<Vec<String>>,
    /// Names aligned with `allargtypes` when present, else with the IN
    /// parameters; NULL unless at least one parameter is named.
    pub argnames: Option<Vec<String>>,
}

/// Look up the single pg_proc row for a managed routine. Returns None when the
/// routine is missing or has unmanaged overloads (pgmg forbids overloading, so
/// more than one row means the catalog doesn't match pgmg's model).
pub async fn get_routine_catalog_info<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
    prokind: &str,
) -> Result<Option<RoutineCatalogInfo>, tokio_postgres::Error> {
    let rows = client.query(
        r#"
        SELECT p.oid,
               p.prorettype::oid AS rettype,
               p.proretset,
               p.proallargtypes::oid[] AS allargtypes,
               p.proargmodes::text[]   AS argmodes,
               p.proargnames           AS argnames
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1 AND p.proname = $2 AND p.prokind = $3::char
        "#,
        &[&schema, &name, &prokind],
    ).await?;

    if rows.len() != 1 {
        return Ok(None);
    }
    let row = &rows[0];
    Ok(Some(RoutineCatalogInfo {
        oid: row.get("oid"),
        rettype: row.get("rettype"),
        retset: row.get("proretset"),
        allargtypes: row.get("allargtypes"),
        argmodes: row.get("argmodes"),
        argnames: row.get("argnames"),
    }))
}

/// Resolve a `name(argtype, ...)` signature to a pg_proc OID via
/// to_regprocedure, which matches identity arguments exactly and normalizes
/// type aliases against the live catalog. None = no such routine.
pub async fn resolve_regprocedure<C: GenericClient>(
    client: &C,
    signature: &str,
) -> Result<Option<u32>, tokio_postgres::Error> {
    let row = client.query_one("SELECT to_regprocedure($1)::oid", &[&signature]).await?;
    Ok(row.get(0))
}

/// Resolve an unambiguous routine name (no argument list) to its result type
/// OID. None when the name doesn't resolve, including when it's overloaded.
pub async fn resolve_regproc_rettype<C: GenericClient>(
    client: &C,
    name: &str,
) -> Result<Option<u32>, tokio_postgres::Error> {
    let rows = client.query(
        "SELECT p.prorettype::oid FROM pg_proc p WHERE p.oid = to_regproc($1)::oid",
        &[&name],
    ).await?;
    Ok(rows.first().map(|r| r.get(0)))
}

/// Resolve type names to OIDs via to_regtype, preserving order. Unresolvable
/// names come back as None.
pub async fn resolve_types<C: GenericClient>(
    client: &C,
    names: &[String],
) -> Result<Vec<Option<u32>>, tokio_postgres::Error> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let rows = client.query(
        r#"
        SELECT to_regtype(u.t)::oid
        FROM unnest($1::text[]) WITH ORDINALITY AS u(t, ord)
        ORDER BY u.ord
        "#,
        &[&names],
    ).await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Output columns of a relation (view, materialized view, or composite type):
/// (attname, atttypid) in attribute order. Empty when the relation is missing.
pub async fn get_relation_shape<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
) -> Result<Vec<(String, u32)>, tokio_postgres::Error> {
    let rows = client.query(
        r#"
        SELECT a.attname, a.atttypid::oid
        FROM pg_attribute a
        JOIN pg_class c     ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        "#,
        &[&schema, &name],
    ).await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Labels of an enum type in sort order. None when the type doesn't exist or
/// isn't an enum.
pub async fn get_enum_labels<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
) -> Result<Option<Vec<String>>, tokio_postgres::Error> {
    let rows = client.query(
        r#"
        SELECT e.enumlabel
        FROM pg_enum e
        JOIN pg_type t      ON t.oid = e.enumtypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = $1 AND t.typname = $2
        ORDER BY e.enumsortorder
        "#,
        &[&schema, &name],
    ).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(rows.iter().map(|r| r.get(0)).collect()))
}

/// Base type OID of a domain. None when the domain doesn't exist.
pub async fn get_domain_base_type<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
) -> Result<Option<u32>, tokio_postgres::Error> {
    let rows = client.query(
        r#"
        SELECT t.typbasetype::oid
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'd'
        "#,
        &[&schema, &name],
    ).await?;
    Ok(rows.first().map(|r| r.get(0)))
}

/// (oprleft, oprright, oprresult) for every operator with this name in the
/// schema. Prefix operators have oprleft = 0.
pub async fn get_operator_signatures<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
) -> Result<Vec<(u32, u32, u32)>, tokio_postgres::Error> {
    let rows = client.query(
        r#"
        SELECT o.oprleft::oid, o.oprright::oid, o.oprresult::oid
        FROM pg_operator o
        JOIN pg_namespace n ON n.oid = o.oprnamespace
        WHERE n.nspname = $1 AND o.oprname = $2
        "#,
        &[&schema, &name],
    ).await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2))).collect())
}
