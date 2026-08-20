//! Client-compatibility severity classification for planned changes.
//!
//! Answers: does applying this change break clients compiled against the
//! current schema? pgmg recreates every changed object with DROP + CREATE, so
//! an update always churns the object's OID; what matters to clients is
//! whether the object's interface — call signature and output row shape —
//! survives the recreation. The old interface comes from the live catalog,
//! the new one from the file DDL (normalized through the same connection so
//! type aliases compare correctly).
//!
//! Classification is infallible: anything that cannot be determined degrades
//! to `Breaking` with a warning, never an error.

use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;
use tracing::warn;

use crate::db::introspect;
use crate::db::introspect::quote_qualified_identifier;
use crate::sql::{ObjectType, SqlObject, split_sql_file};
use crate::sql::objects::extract_type_name;

/// Client-compatibility impact of a planned change. Variant order gives the
/// `Ord`: Safe < Transient < Breaking, so a plan's rollup is `max()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Old clients unaffected.
    Safe,
    /// Old clients briefly error until statement caches flush: the interface
    /// is unchanged but the object's OID churns.
    Transient,
    /// Old clients permanently broken until redeployed.
    Breaking,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeverityCounts {
    pub safe: usize,
    pub transient: usize,
    pub breaking: usize,
}

impl SeverityCounts {
    pub fn record(&mut self, severity: Severity) {
        match severity {
            Severity::Safe => self.safe += 1,
            Severity::Transient => self.transient += 1,
            Severity::Breaking => self.breaking += 1,
        }
    }
}

/// True if clients can be compiled against this kind of object. Dropping or
/// reshaping a non-referencable object changes performance or runtime side
/// effects but can never produce a client error.
pub fn is_client_referencable(object_type: &ObjectType) -> bool {
    !matches!(
        object_type,
        ObjectType::Comment | ObjectType::Index | ObjectType::Trigger | ObjectType::CronJob
    )
}

/// A brand-new object: nothing referenced it before, so old clients are
/// unaffected regardless of kind.
pub fn classify_create(_object: &SqlObject) -> Severity {
    Severity::Safe
}

pub fn classify_delete(object_type: &ObjectType, _object_name: &str) -> Severity {
    if is_client_referencable(object_type) {
        Severity::Breaking
    } else {
        Severity::Safe
    }
}

/// Classify an update, which pgmg always applies as DROP + CREATE — so the
/// best case for a client-referencable object is Transient (OID churn with an
/// identical interface).
pub async fn classify_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    match object.object_type {
        // Metadata-only: comments are re-set in place, never dropped.
        ObjectType::Comment => Severity::Safe,
        // No client-referencable interface; recreation affects perf/behavior only.
        ObjectType::Index | ObjectType::Trigger | ObjectType::CronJob => Severity::Safe,
        // A declaratively-managed table update is DROP TABLE + CREATE TABLE.
        ObjectType::Table => Severity::Breaking,
        ObjectType::Function | ObjectType::Procedure => classify_routine_update(client, object).await,
        ObjectType::Aggregate => classify_aggregate_update(client, object).await,
        ObjectType::View | ObjectType::MaterializedView => {
            classify_relation_shape_update(client, object).await
        }
        ObjectType::Type => classify_type_update(client, object).await,
        ObjectType::Domain => classify_domain_update(client, object).await,
        ObjectType::Operator => classify_operator_update(client, object).await,
    }
}

fn breaking_warn(object: &SqlObject, why: &str) -> Severity {
    warn!(
        object_type = ?object.object_type,
        object = %crate::sql::format_qualified_name(&object.qualified_name),
        "cannot determine client impact ({}); assuming breaking",
        why
    );
    Severity::Breaking
}

fn object_schema(object: &SqlObject) -> &str {
    object.qualified_name.schema.as_deref().unwrap_or("public")
}

// ---------------------------------------------------------------------------
// Functions and procedures
// ---------------------------------------------------------------------------

/// The new routine interface as written in the file DDL.
#[derive(Debug, Default, PartialEq)]
struct RoutineInterface {
    /// Type names of identity (IN/INOUT/VARIADIC) parameters, in order.
    identity_types: Vec<String>,
    /// Names of identity parameters, positionally aligned with
    /// `identity_types`; empty string for unnamed parameters.
    identity_names: Vec<String>,
    /// (name, type name) of OUT/INOUT/TABLE parameters, in order.
    out_cols: Vec<(String, String)>,
    /// The RETURNS clause type, when present.
    ret_type: Option<String>,
    /// RETURNS SETOF or RETURNS TABLE.
    setof: bool,
}

fn extract_routine_interface(ddl: &str) -> Option<RoutineInterface> {
    let parsed = pg_query::parse(ddl).ok()?;
    let stmt = parsed.protobuf.stmts.first()?.stmt.as_ref()?;
    let Some(pg_query::NodeEnum::CreateFunctionStmt(func)) = &stmt.node else {
        return None;
    };

    use pg_query::protobuf::FunctionParameterMode as Mode;
    let mut iface = RoutineInterface::default();
    for param in &func.parameters {
        let Some(pg_query::NodeEnum::FunctionParameter(fp)) = &param.node else {
            return None;
        };
        let type_text = fp.arg_type.as_ref().filter(|t| !t.pct_type).and_then(|t| extract_type_name(t))?;
        match fp.mode() {
            Mode::FuncParamIn | Mode::FuncParamVariadic | Mode::FuncParamDefault => {
                iface.identity_types.push(type_text);
                iface.identity_names.push(fp.name.clone());
            }
            Mode::FuncParamInout => {
                iface.identity_types.push(type_text.clone());
                iface.identity_names.push(fp.name.clone());
                iface.out_cols.push((fp.name.clone(), type_text));
            }
            Mode::FuncParamOut => {
                iface.out_cols.push((fp.name.clone(), type_text));
            }
            Mode::FuncParamTable => {
                iface.out_cols.push((fp.name.clone(), type_text));
                iface.setof = true;
            }
            Mode::Undefined => return None,
        }
    }
    if let Some(ret) = &func.return_type {
        if ret.pct_type {
            return None;
        }
        iface.ret_type = Some(extract_type_name(ret)?);
        iface.setof |= ret.setof;
    }
    Some(iface)
}

/// Old-side parameter view derived from a pg_proc row: identity-parameter
/// names and the OUT/INOUT/TABLE columns as (name, type OID).
fn old_param_lists(info: &introspect::RoutineCatalogInfo) -> (Vec<String>, Vec<(String, u32)>) {
    let name_at = |i: usize| -> String {
        info.argnames
            .as_ref()
            .and_then(|names| names.get(i))
            .cloned()
            .unwrap_or_default()
    };

    match (&info.allargtypes, &info.argmodes) {
        (Some(types), Some(modes)) => {
            let mut in_names = Vec::new();
            let mut out_cols = Vec::new();
            for (i, (oid, mode)) in types.iter().zip(modes.iter()).enumerate() {
                if matches!(mode.as_str(), "i" | "b" | "v") {
                    in_names.push(name_at(i));
                }
                if matches!(mode.as_str(), "o" | "b" | "t") {
                    out_cols.push((name_at(i), *oid));
                }
            }
            (in_names, out_cols)
        }
        // All parameters are plain IN; argnames aligns with them directly.
        _ => (info.argnames.clone().unwrap_or_default(), Vec::new()),
    }
}

async fn classify_routine_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    let Some(new) = extract_routine_interface(&object.ddl_statement) else {
        return breaking_warn(object, "could not parse routine interface from DDL");
    };
    let prokind = if object.object_type == ObjectType::Procedure { "p" } else { "f" };
    classify_routine_against_catalog(client, object, prokind, &new).await
}

async fn classify_routine_against_catalog<C: GenericClient>(
    client: &C,
    object: &SqlObject,
    prokind: &str,
    new: &RoutineInterface,
) -> Severity {
    let schema = object_schema(object);
    let info = match introspect::get_routine_catalog_info(client, schema, &object.qualified_name.name, prokind).await {
        Ok(Some(info)) => info,
        Ok(None) => return breaking_warn(object, "existing routine not found (or overloaded) in catalog"),
        Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
    };

    // Identity arguments: resolve the new signature through to_regprocedure;
    // hitting the same OID means the identity argument types are unchanged.
    let signature = format!(
        "{}({})",
        quote_qualified_identifier(Some(schema), &object.qualified_name.name),
        new.identity_types.join(",")
    );
    match introspect::resolve_regprocedure(client, &signature).await {
        Ok(Some(oid)) if oid == info.oid => {}
        Ok(_) => return Severity::Breaking, // signature no longer matches the live routine
        Err(e) => return breaking_warn(object, &format!("signature resolution failed: {e}")),
    }

    // Parameter renames break clients using named notation. Only names that
    // existed before are load-bearing; naming a previously unnamed parameter
    // is fine.
    let (old_in_names, old_out_cols) = old_param_lists(&info);
    for (old_name, new_name) in old_in_names.iter().zip(new.identity_names.iter()) {
        if !old_name.is_empty() && old_name != new_name {
            return Severity::Breaking;
        }
    }

    // Result shape.
    if new.setof != info.retset {
        return Severity::Breaking;
    }
    if !old_out_cols.is_empty() || !new.out_cols.is_empty() {
        let new_types: Vec<String> = new.out_cols.iter().map(|(_, t)| t.clone()).collect();
        let new_oids = match introspect::resolve_types(client, &new_types).await {
            Ok(oids) => oids,
            Err(e) => return breaking_warn(object, &format!("type resolution failed: {e}")),
        };
        if new_oids.iter().any(|o| o.is_none()) {
            return breaking_warn(object, "could not resolve an output parameter type");
        }
        let new_out: Vec<(String, u32)> = new
            .out_cols
            .iter()
            .zip(new_oids)
            .map(|((name, _), oid)| (name.clone(), oid.unwrap()))
            .collect();
        if old_out_cols != new_out {
            return Severity::Breaking;
        }
    } else {
        match &new.ret_type {
            Some(ret) => {
                match introspect::resolve_types(client, std::slice::from_ref(ret)).await {
                    Ok(oids) => match oids.first().copied().flatten() {
                        Some(oid) if oid == info.rettype => {}
                        Some(_) => return Severity::Breaking,
                        None => return breaking_warn(object, "could not resolve the return type"),
                    },
                    Err(e) => return breaking_warn(object, &format!("type resolution failed: {e}")),
                }
            }
            // Procedures have no RETURNS clause; both sides are void.
            None if prokind == "p" => {}
            None => return breaking_warn(object, "routine has no parseable return type"),
        }
    }

    Severity::Transient
}

// ---------------------------------------------------------------------------
// Aggregates
// ---------------------------------------------------------------------------

/// Type names of an aggregate's input arguments from a CREATE AGGREGATE
/// DefineStmt, plus the type names/functions determining its result.
fn extract_aggregate_interface(ddl: &str) -> Option<(Vec<String>, Option<String>, Option<String>)> {
    let parsed = pg_query::parse(ddl).ok()?;
    let stmt = parsed.protobuf.stmts.first()?.stmt.as_ref()?;
    let Some(pg_query::NodeEnum::DefineStmt(define)) = &stmt.node else {
        return None;
    };
    if define.kind != 2 {
        return None; // OBJECT_AGGREGATE
    }

    // args is a 2-element list: [List of FunctionParameter, Integer numDirectArgs]
    let mut input_types = Vec::new();
    for node in &define.args {
        let params: &[pg_query::protobuf::Node] = match &node.node {
            Some(pg_query::NodeEnum::List(list)) => &list.items,
            Some(pg_query::NodeEnum::FunctionParameter(_)) => std::slice::from_ref(node),
            _ => continue,
        };
        for param in params {
            if let Some(pg_query::NodeEnum::FunctionParameter(fp)) = &param.node {
                let type_text = fp.arg_type.as_ref().filter(|t| !t.pct_type).and_then(|t| extract_type_name(t))?;
                input_types.push(type_text);
            }
        }
    }

    let mut finalfunc = None;
    let mut stype = None;
    for def_elem in &define.definition {
        if let Some(pg_query::NodeEnum::DefElem(elem)) = &def_elem.node {
            let value = elem.arg.as_ref().and_then(|arg| match &arg.node {
                Some(pg_query::NodeEnum::TypeName(t)) => extract_type_name(t),
                Some(pg_query::NodeEnum::String(s)) => Some(s.sval.clone()),
                _ => None,
            });
            match elem.defname.to_lowercase().as_str() {
                "finalfunc" => finalfunc = value,
                "stype" => stype = value,
                _ => {}
            }
        }
    }

    Some((input_types, finalfunc, stype))
}

async fn classify_aggregate_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    let Some((input_types, finalfunc, stype)) = extract_aggregate_interface(&object.ddl_statement) else {
        return breaking_warn(object, "could not parse aggregate interface from DDL");
    };
    let schema = object_schema(object);
    let info = match introspect::get_routine_catalog_info(client, schema, &object.qualified_name.name, "a").await {
        Ok(Some(info)) => info,
        Ok(None) => return breaking_warn(object, "existing aggregate not found (or overloaded) in catalog"),
        Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
    };

    let signature = format!(
        "{}({})",
        quote_qualified_identifier(Some(schema), &object.qualified_name.name),
        input_types.join(",")
    );
    match introspect::resolve_regprocedure(client, &signature).await {
        Ok(Some(oid)) if oid == info.oid => {}
        Ok(_) => return Severity::Breaking,
        Err(e) => return breaking_warn(object, &format!("signature resolution failed: {e}")),
    }

    // Result type: the final function's return type, or the state type when
    // there is no final function. The final function is resolved against the
    // pre-apply catalog, so this is a best-effort comparison.
    let new_rettype = match (finalfunc, stype) {
        (Some(func), _) => match introspect::resolve_regproc_rettype(client, &func).await {
            Ok(Some(oid)) => oid,
            Ok(None) => return breaking_warn(object, "could not resolve the aggregate's final function"),
            Err(e) => return breaking_warn(object, &format!("final function lookup failed: {e}")),
        },
        (None, Some(stype)) => match introspect::resolve_types(client, std::slice::from_ref(&stype)).await {
            Ok(oids) => match oids.first().copied().flatten() {
                Some(oid) => oid,
                None => return breaking_warn(object, "could not resolve the aggregate's state type"),
            },
            Err(e) => return breaking_warn(object, &format!("type resolution failed: {e}")),
        },
        (None, None) => return breaking_warn(object, "aggregate has no finalfunc or stype"),
    };
    if new_rettype != info.rettype {
        return Severity::Breaking;
    }

    Severity::Transient
}

// ---------------------------------------------------------------------------
// Views and materialized views
// ---------------------------------------------------------------------------

/// The defining SELECT of a view/matview DDL plus any explicit column aliases
/// (`CREATE VIEW v (a, b) AS ...`).
fn extract_view_query(ddl: &str) -> Option<(String, Vec<String>)> {
    let parsed = pg_query::parse(ddl).ok()?;
    let stmt = parsed.protobuf.stmts.first()?.stmt.as_ref()?;
    let (query_node, alias_nodes) = match &stmt.node {
        Some(pg_query::NodeEnum::ViewStmt(view)) => {
            (view.query.as_deref()?, view.aliases.as_slice())
        }
        Some(pg_query::NodeEnum::CreateTableAsStmt(ctas)) if ctas.objtype == 24 => {
            let col_names = ctas.into.as_ref().map(|i| i.col_names.as_slice()).unwrap_or(&[]);
            (ctas.query.as_deref()?, col_names)
        }
        _ => return None,
    };
    let select_sql = query_node.node.clone()?.deparse().ok()?;
    let aliases = alias_nodes
        .iter()
        .filter_map(|n| match &n.node {
            Some(pg_query::NodeEnum::String(s)) => Some(s.sval.clone()),
            _ => None,
        })
        .collect();
    Some((select_sql, aliases))
}

async fn classify_relation_shape_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    let schema = object_schema(object);
    let old_cols = match introspect::get_relation_shape(client, schema, &object.qualified_name.name).await {
        Ok(cols) => cols,
        Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
    };
    if old_cols.is_empty() {
        return breaking_warn(object, "existing relation not found in catalog");
    }

    let Some((select_sql, aliases)) = extract_view_query(&object.ddl_statement) else {
        return breaking_warn(object, "could not extract the defining query from DDL");
    };

    // Preparing the defining SELECT yields exactly the Describe a client
    // would see. This reflects pre-apply state: if the query references
    // objects this same plan changes, the probe can fail or be optimistic —
    // the root-cause operation carries its own severity either way.
    let stmt = match client.prepare(&select_sql).await {
        Ok(stmt) => stmt,
        Err(e) => {
            return breaking_warn(
                object,
                &format!("could not prepare the defining query (it may reference objects changed by this plan): {e}"),
            )
        }
    };
    let mut new_cols: Vec<(String, u32)> = stmt
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_().oid()))
        .collect();
    for (i, alias) in aliases.iter().enumerate() {
        if let Some(col) = new_cols.get_mut(i) {
            col.0 = alias.clone();
        }
    }

    if old_cols == new_cols {
        Severity::Transient
    } else {
        Severity::Breaking
    }
}

// ---------------------------------------------------------------------------
// Types (enums and composites), domains, operators
// ---------------------------------------------------------------------------

/// True if `old` appears within `new` in order (labels may be inserted
/// anywhere, but every existing label survives with relative order intact).
fn is_ordered_subsequence(old: &[String], new: &[String]) -> bool {
    let mut new_iter = new.iter();
    old.iter().all(|label| new_iter.any(|candidate| candidate == label))
}

async fn classify_type_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    let parsed = match pg_query::parse(&object.ddl_statement) {
        Ok(p) => p,
        Err(e) => return breaking_warn(object, &format!("could not parse type DDL: {e}")),
    };
    let node = parsed
        .protobuf
        .stmts
        .first()
        .and_then(|s| s.stmt.as_ref())
        .and_then(|s| s.node.as_ref());
    let schema = object_schema(object);

    match node {
        Some(pg_query::NodeEnum::CreateEnumStmt(enum_stmt)) => {
            let new_labels: Vec<String> = enum_stmt
                .vals
                .iter()
                .filter_map(|n| match &n.node {
                    Some(pg_query::NodeEnum::String(s)) => Some(s.sval.clone()),
                    _ => None,
                })
                .collect();
            let old_labels = match introspect::get_enum_labels(client, schema, &object.qualified_name.name).await {
                Ok(Some(labels)) => labels,
                Ok(None) => return breaking_warn(object, "existing enum not found in catalog (or type is not an enum)"),
                Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
            };
            // Appending labels leaves every old value valid; removal, rename,
            // or reorder of existing labels breaks clients.
            if is_ordered_subsequence(&old_labels, &new_labels) {
                Severity::Transient
            } else {
                Severity::Breaking
            }
        }
        Some(pg_query::NodeEnum::CompositeTypeStmt(comp)) => {
            let mut new_attrs: Vec<(String, String)> = Vec::new();
            for coldef in &comp.coldeflist {
                let Some(pg_query::NodeEnum::ColumnDef(col)) = &coldef.node else {
                    return breaking_warn(object, "unrecognized attribute in composite type DDL");
                };
                let Some(type_text) = col.type_name.as_ref().filter(|t| !t.pct_type).and_then(|t| extract_type_name(t)) else {
                    return breaking_warn(object, "could not resolve a composite attribute type");
                };
                new_attrs.push((col.colname.clone(), type_text));
            }
            let old_attrs = match introspect::get_relation_shape(client, schema, &object.qualified_name.name).await {
                Ok(attrs) => attrs,
                Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
            };
            if old_attrs.is_empty() {
                return breaking_warn(object, "existing composite type not found in catalog");
            }
            let type_texts: Vec<String> = new_attrs.iter().map(|(_, t)| t.clone()).collect();
            let oids = match introspect::resolve_types(client, &type_texts).await {
                Ok(oids) => oids,
                Err(e) => return breaking_warn(object, &format!("type resolution failed: {e}")),
            };
            if oids.iter().any(|o| o.is_none()) {
                return breaking_warn(object, "could not resolve a composite attribute type");
            }
            let new_shape: Vec<(String, u32)> = new_attrs
                .into_iter()
                .zip(oids)
                .map(|((name, _), oid)| (name, oid.unwrap()))
                .collect();
            if old_attrs == new_shape {
                Severity::Transient
            } else {
                Severity::Breaking
            }
        }
        _ => breaking_warn(object, "unrecognized TYPE DDL"),
    }
}

async fn classify_domain_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    let parsed = match pg_query::parse(&object.ddl_statement) {
        Ok(p) => p,
        Err(e) => return breaking_warn(object, &format!("could not parse domain DDL: {e}")),
    };
    let Some(pg_query::NodeEnum::CreateDomainStmt(domain)) = parsed
        .protobuf
        .stmts
        .first()
        .and_then(|s| s.stmt.as_ref())
        .and_then(|s| s.node.as_ref())
    else {
        return breaking_warn(object, "unrecognized domain DDL");
    };
    let Some(base_type) = domain.type_name.as_ref().filter(|t| !t.pct_type).and_then(|t| extract_type_name(t)) else {
        return breaking_warn(object, "could not resolve the domain base type");
    };

    let schema = object_schema(object);
    let old_base = match introspect::get_domain_base_type(client, schema, &object.qualified_name.name).await {
        Ok(Some(oid)) => oid,
        Ok(None) => return breaking_warn(object, "existing domain not found in catalog"),
        Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
    };
    let new_base = match introspect::resolve_types(client, std::slice::from_ref(&base_type)).await {
        Ok(oids) => match oids.first().copied().flatten() {
            Some(oid) => oid,
            None => return breaking_warn(object, "could not resolve the domain base type"),
        },
        Err(e) => return breaking_warn(object, &format!("type resolution failed: {e}")),
    };

    // Constraint changes (CHECK/NOT NULL) affect writes at runtime but not
    // compiled clients; only the base type is part of the wire interface.
    if old_base == new_base {
        Severity::Transient
    } else {
        Severity::Breaking
    }
}

async fn classify_operator_update<C: GenericClient>(client: &C, object: &SqlObject) -> Severity {
    let parsed = match pg_query::parse(&object.ddl_statement) {
        Ok(p) => p,
        Err(e) => return breaking_warn(object, &format!("could not parse operator DDL: {e}")),
    };
    let Some(pg_query::NodeEnum::DefineStmt(define)) = parsed
        .protobuf
        .stmts
        .first()
        .and_then(|s| s.stmt.as_ref())
        .and_then(|s| s.node.as_ref())
    else {
        return breaking_warn(object, "unrecognized operator DDL");
    };
    if define.kind != 26 {
        return breaking_warn(object, "unrecognized operator DDL");
    }

    let mut left_type = None;
    let mut right_type = None;
    let mut procedure = None;
    for def_elem in &define.definition {
        if let Some(pg_query::NodeEnum::DefElem(elem)) = &def_elem.node {
            let type_text = elem.arg.as_ref().and_then(|arg| match &arg.node {
                Some(pg_query::NodeEnum::TypeName(t)) => extract_type_name(t),
                Some(pg_query::NodeEnum::ObjectWithArgs(f)) => {
                    let parts: Vec<String> = f
                        .objname
                        .iter()
                        .filter_map(|n| match &n.node {
                            Some(pg_query::NodeEnum::String(s)) => Some(s.sval.clone()),
                            _ => None,
                        })
                        .collect();
                    Some(parts.join("."))
                }
                Some(pg_query::NodeEnum::String(s)) => Some(s.sval.clone()),
                _ => None,
            });
            match elem.defname.to_lowercase().as_str() {
                "leftarg" => left_type = type_text,
                "rightarg" => right_type = type_text,
                "procedure" | "function" => procedure = type_text,
                _ => {}
            }
        }
    }

    // Resolve the new operand types; a missing arg (prefix operator) matches
    // the catalog's oprleft/oprright = 0 convention.
    let operand_texts: Vec<String> = [&left_type, &right_type].iter().filter_map(|t| (*t).clone()).collect();
    let resolved = match introspect::resolve_types(client, &operand_texts).await {
        Ok(oids) => oids,
        Err(e) => return breaking_warn(object, &format!("type resolution failed: {e}")),
    };
    let mut resolved_iter = resolved.into_iter();
    let mut next_operand = |present: bool| -> Option<u32> {
        if present { resolved_iter.next().flatten() } else { Some(0) }
    };
    let Some(new_left) = next_operand(left_type.is_some()) else {
        return breaking_warn(object, "could not resolve the operator's left operand type");
    };
    let Some(new_right) = next_operand(right_type.is_some()) else {
        return breaking_warn(object, "could not resolve the operator's right operand type");
    };

    let schema = object_schema(object);
    let existing = match introspect::get_operator_signatures(client, schema, &object.qualified_name.name).await {
        Ok(rows) => rows,
        Err(e) => return breaking_warn(object, &format!("catalog lookup failed: {e}")),
    };
    if existing.is_empty() {
        return breaking_warn(object, "existing operator not found in catalog");
    }
    let Some((_, _, old_result)) = existing.iter().find(|(l, r, _)| *l == new_left && *r == new_right) else {
        return Severity::Breaking; // operand types changed
    };

    // Result type isn't in the CREATE OPERATOR AST; derive it from the
    // implementing function when it resolves in the pre-apply catalog. If the
    // function is itself changing in this plan, its own operation carries the
    // severity.
    if let Some(procedure) = procedure {
        if let Ok(Some(rettype)) = introspect::resolve_regproc_rettype(client, &procedure).await {
            if rettype != *old_result {
                return Severity::Breaking;
            }
        }
    }

    Severity::Transient
}

// ---------------------------------------------------------------------------
// Migrations
// ---------------------------------------------------------------------------

/// Classify a raw SQL migration by classifying each statement and taking the
/// worst. Pure AST work — no database needed.
pub fn classify_migration(name: &str, content: &str) -> Severity {
    let statements = match split_sql_file(content) {
        Ok(statements) => statements,
        Err(e) => {
            warn!(migration = name, "could not split migration ({}); assuming breaking", e);
            return Severity::Breaking;
        }
    };
    statements
        .iter()
        .map(|stmt| classify_migration_statement(name, &stmt.sql))
        .max()
        .unwrap_or(Severity::Safe)
}

fn classify_migration_statement(migration: &str, sql: &str) -> Severity {
    let parsed = match pg_query::parse(sql) {
        Ok(parsed) => parsed,
        Err(e) => {
            warn!(
                migration,
                statement = statement_preview(sql),
                "unparseable migration statement ({}); assuming breaking",
                e
            );
            return Severity::Breaking;
        }
    };

    parsed
        .protobuf
        .stmts
        .iter()
        .filter_map(|s| s.stmt.as_ref().and_then(|s| s.node.as_ref()))
        .map(|node| classify_migration_node(migration, sql, node))
        .max()
        .unwrap_or(Severity::Safe)
}

fn classify_migration_node(migration: &str, sql: &str, node: &pg_query::NodeEnum) -> Severity {
    use pg_query::NodeEnum::*;
    match node {
        // Purely additive DDL, metadata, and DML: old clients keep working.
        // (TRUNCATE destroys data but leaves the schema interface intact.)
        CreateStmt(_) | IndexStmt(_) | CreateSeqStmt(_) | CommentStmt(_)
        | CreateExtensionStmt(_) | CreateSchemaStmt(_) | ViewStmt(_)
        | CreateTableAsStmt(_) | CreateFunctionStmt(_) | CreateTrigStmt(_)
        | CreateEnumStmt(_) | CompositeTypeStmt(_) | CreateDomainStmt(_)
        | DefineStmt(_) | InsertStmt(_) | UpdateStmt(_) | DeleteStmt(_)
        | SelectStmt(_) | CopyStmt(_) | VariableSetStmt(_) | TruncateStmt(_)
        | ClusterStmt(_) | VacuumStmt(_) | AlterOwnerStmt(_) => Severity::Safe,
        GrantStmt(grant) if grant.is_grant => Severity::Safe,
        // ALTER TYPE ... ADD VALUE only extends the value set.
        AlterEnumStmt(alter) if !alter.new_val.is_empty() && alter.old_val.is_empty() => Severity::Safe,
        // Anything reshaping or removing existing schema. ALTER TABLE is
        // Breaking in every form — even ADD COLUMN changes the row shape.
        AlterTableStmt(_) | DropStmt(_) | RenameStmt(_) | GrantStmt(_) | AlterEnumStmt(_) => {
            Severity::Breaking
        }
        _ => {
            warn!(
                migration,
                statement = statement_preview(sql),
                "unclassified migration statement kind; assuming breaking"
            );
            Severity::Breaking
        }
    }
}

fn statement_preview(sql: &str) -> String {
    let trimmed = sql.trim().replace('\n', " ");
    if trimmed.len() > 80 {
        format!("{}…", &trimmed[..trimmed.char_indices().take(80).last().map(|(i, c)| i + c.len_utf8()).unwrap_or(0)])
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql_object(object_type: ObjectType, name: &str, ddl: &str) -> SqlObject {
        SqlObject::new(
            object_type,
            crate::sql::QualifiedIdent::from_qualified_name(name),
            ddl.to_string(),
            crate::sql::Dependencies::default(),
            None,
        )
    }

    #[test]
    fn severity_ordering_and_rollup() {
        assert!(Severity::Safe < Severity::Transient);
        assert!(Severity::Transient < Severity::Breaking);
        let rollup = [Severity::Safe, Severity::Transient, Severity::Safe]
            .into_iter()
            .max()
            .unwrap();
        assert_eq!(rollup, Severity::Transient);
    }

    #[test]
    fn severity_counts_record() {
        let mut counts = SeverityCounts::default();
        counts.record(Severity::Safe);
        counts.record(Severity::Breaking);
        counts.record(Severity::Breaking);
        assert_eq!(counts, SeverityCounts { safe: 1, transient: 0, breaking: 2 });
    }

    #[test]
    fn client_referencable_table() {
        assert!(is_client_referencable(&ObjectType::Table));
        assert!(is_client_referencable(&ObjectType::View));
        assert!(is_client_referencable(&ObjectType::Function));
        assert!(is_client_referencable(&ObjectType::Domain));
        assert!(!is_client_referencable(&ObjectType::Index));
        assert!(!is_client_referencable(&ObjectType::Trigger));
        assert!(!is_client_referencable(&ObjectType::Comment));
        assert!(!is_client_referencable(&ObjectType::CronJob));
    }

    #[test]
    fn delete_severity_follows_referencability() {
        assert_eq!(classify_delete(&ObjectType::View, "public.v"), Severity::Breaking);
        assert_eq!(classify_delete(&ObjectType::Function, "public.f()"), Severity::Breaking);
        assert_eq!(classify_delete(&ObjectType::Index, "idx"), Severity::Safe);
        assert_eq!(classify_delete(&ObjectType::Trigger, "trg:t"), Severity::Safe);
        assert_eq!(classify_delete(&ObjectType::Comment, "table:t"), Severity::Safe);
        assert_eq!(classify_delete(&ObjectType::CronJob, "job"), Severity::Safe);
    }

    #[test]
    fn create_is_always_safe() {
        let obj = sql_object(ObjectType::Table, "public.t", "CREATE TABLE public.t (id int)");
        assert_eq!(classify_create(&obj), Severity::Safe);
    }

    #[test]
    fn migration_create_table_is_safe() {
        assert_eq!(
            classify_migration("m", "CREATE TABLE users (id serial primary key);"),
            Severity::Safe
        );
    }

    #[test]
    fn migration_dml_and_index_are_safe() {
        let sql = r#"
            INSERT INTO users (name) VALUES ('a');
            UPDATE users SET name = 'b';
            CREATE INDEX idx_users_name ON users (name);
            COMMENT ON TABLE users IS 'people';
            GRANT SELECT ON users TO reporting;
        "#;
        assert_eq!(classify_migration("m", sql), Severity::Safe);
    }

    #[test]
    fn migration_alter_table_is_breaking() {
        assert_eq!(
            classify_migration("m", "ALTER TABLE users ADD COLUMN age int;"),
            Severity::Breaking
        );
    }

    #[test]
    fn migration_drop_is_breaking() {
        assert_eq!(classify_migration("m", "DROP TABLE users;"), Severity::Breaking);
    }

    #[test]
    fn migration_rename_is_breaking() {
        assert_eq!(
            classify_migration("m", "ALTER TABLE users RENAME TO people;"),
            Severity::Breaking
        );
    }

    #[test]
    fn migration_revoke_is_breaking() {
        assert_eq!(
            classify_migration("m", "REVOKE SELECT ON users FROM reporting;"),
            Severity::Breaking
        );
    }

    #[test]
    fn migration_enum_add_value_is_safe() {
        assert_eq!(
            classify_migration("m", "ALTER TYPE status ADD VALUE 'archived';"),
            Severity::Safe
        );
    }

    #[test]
    fn migration_enum_rename_value_is_breaking() {
        assert_eq!(
            classify_migration("m", "ALTER TYPE status RENAME VALUE 'old' TO 'new';"),
            Severity::Breaking
        );
    }

    #[test]
    fn migration_unknown_statement_is_breaking() {
        assert_eq!(
            classify_migration("m", "DO $$ BEGIN NULL; END $$;"),
            Severity::Breaking
        );
    }

    #[test]
    fn migration_unparseable_is_breaking() {
        assert_eq!(classify_migration("m", "THIS IS NOT SQL;"), Severity::Breaking);
    }

    #[test]
    fn migration_mixed_takes_max() {
        let sql = r#"
            CREATE TABLE audit (id int);
            ALTER TABLE users ADD COLUMN age int;
        "#;
        assert_eq!(classify_migration("m", sql), Severity::Breaking);
    }

    #[test]
    fn migration_empty_is_safe() {
        assert_eq!(classify_migration("m", "\n-- nothing here\n"), Severity::Safe);
    }

    #[test]
    fn ordered_subsequence() {
        let old = vec!["a".to_string(), "b".to_string()];
        let appended = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let inserted = vec!["a".to_string(), "x".to_string(), "b".to_string()];
        let reordered = vec!["b".to_string(), "a".to_string()];
        let removed = vec!["a".to_string()];
        assert!(is_ordered_subsequence(&old, &appended));
        assert!(is_ordered_subsequence(&old, &inserted));
        assert!(!is_ordered_subsequence(&old, &reordered));
        assert!(!is_ordered_subsequence(&old, &removed));
    }

    #[test]
    fn extract_routine_interface_scalar() {
        let iface = extract_routine_interface(
            "CREATE FUNCTION add(a int, b int) RETURNS bigint LANGUAGE sql AS $$ SELECT a + b $$",
        )
        .unwrap();
        assert_eq!(iface.identity_types, vec!["pg_catalog.int4", "pg_catalog.int4"]);
        assert_eq!(iface.identity_names, vec!["a", "b"]);
        assert!(iface.out_cols.is_empty());
        assert_eq!(iface.ret_type.as_deref(), Some("pg_catalog.int8"));
        assert!(!iface.setof);
    }

    #[test]
    fn extract_routine_interface_returns_table() {
        let iface = extract_routine_interface(
            "CREATE FUNCTION list_users() RETURNS TABLE(id int, name text) LANGUAGE sql AS $$ SELECT 1, 'x' $$",
        )
        .unwrap();
        assert!(iface.identity_types.is_empty());
        assert_eq!(
            iface.out_cols,
            vec![
                ("id".to_string(), "pg_catalog.int4".to_string()),
                ("name".to_string(), "text".to_string()),
            ]
        );
        assert!(iface.setof);
    }

    #[test]
    fn extract_routine_interface_out_params() {
        let iface = extract_routine_interface(
            "CREATE FUNCTION stats(IN q text, OUT total int, OUT avg numeric) LANGUAGE sql AS $$ SELECT 1, 2.0 $$",
        )
        .unwrap();
        assert_eq!(iface.identity_types, vec!["text"]);
        assert_eq!(
            iface.out_cols,
            vec![
                ("total".to_string(), "pg_catalog.int4".to_string()),
                ("avg".to_string(), "pg_catalog.numeric".to_string()),
            ]
        );
    }

    #[test]
    fn extract_routine_interface_setof() {
        let iface = extract_routine_interface(
            "CREATE FUNCTION all_ids() RETURNS SETOF bigint LANGUAGE sql AS $$ SELECT 1 $$",
        )
        .unwrap();
        assert!(iface.setof);
    }

    #[test]
    fn extract_routine_interface_array_type() {
        let iface = extract_routine_interface(
            "CREATE FUNCTION tags(ids int[]) RETURNS text[] LANGUAGE sql AS $$ SELECT ARRAY['a'] $$",
        )
        .unwrap();
        assert_eq!(iface.identity_types, vec!["pg_catalog.int4[]"]);
        assert_eq!(iface.ret_type.as_deref(), Some("text[]"));
    }

    #[test]
    fn extract_view_query_plain() {
        let (sql, aliases) =
            extract_view_query("CREATE VIEW v AS SELECT id, name FROM users").unwrap();
        assert!(sql.to_uppercase().starts_with("SELECT"));
        assert!(aliases.is_empty());
    }

    #[test]
    fn extract_view_query_with_aliases() {
        let (_, aliases) =
            extract_view_query("CREATE VIEW v (a, b) AS SELECT id, name FROM users").unwrap();
        assert_eq!(aliases, vec!["a", "b"]);
    }

    #[test]
    fn extract_view_query_matview() {
        let (sql, _) =
            extract_view_query("CREATE MATERIALIZED VIEW mv AS SELECT id FROM users").unwrap();
        assert!(sql.to_uppercase().starts_with("SELECT"));
    }

    #[test]
    fn extract_aggregate_interface_basic() {
        let (inputs, finalfunc, stype) = extract_aggregate_interface(
            "CREATE AGGREGATE my_sum (int) (SFUNC = int4pl, STYPE = int4)",
        )
        .unwrap();
        assert_eq!(inputs, vec!["pg_catalog.int4"]);
        assert!(finalfunc.is_none());
        // Spelled as an identifier, not the `int` keyword, so no pg_catalog prefix
        assert_eq!(stype.as_deref(), Some("int4"));
    }
}
