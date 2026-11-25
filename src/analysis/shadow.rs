//! Post-apply view shapes, worked out without applying anything.
//!
//! Severity classification compares a view's live columns with the columns
//! its new definition produces, by preparing the new defining query. That
//! query runs against the database as it is before the apply, so it cannot be
//! prepared when it uses something the same apply introduces: a column a
//! pending migration adds, or a column a view upstream of it gains. The most
//! common additive change, "add a column and expose it in the view", is
//! exactly that case.
//!
//! This module builds stand-ins for what the apply will produce and reads the
//! shapes off them:
//!
//! - for every table a pending migration adds columns to, an empty copy of
//!   the table with those columns added (and the types the migrations create,
//!   so a new column can be of a new type);
//! - for every table a pending migration creates, an empty table with the
//!   same columns;
//! - for every view or materialized view the plan creates or updates, a plain
//!   view with the new definition, pointed at the stand-ins.
//!
//! Stand-ins live in scratch schemas inside a transaction that is always
//! rolled back, so nothing persists and no lock stronger than `ACCESS SHARE`
//! is taken on an existing object. Anything that cannot be built is simply left
//! out, and the caller keeps its conservative answer for it.
//!
//! Only additions are mirrored. A migration that drops, renames or retypes a
//! column is not, which is sound because such a migration is classified
//! `Breaking` on its own.

use std::collections::{HashMap, HashSet};

use pg_query::protobuf::{AlterTableType, ObjectType as PgObjectType, Token};
use pg_query::NodeEnum;
use tokio_postgres::Client;
use tracing::debug;

use crate::db::introspect::{self, quote_identifier};
use crate::sql::late_binding::identifier_text;
use crate::sql::SqlObject;

const SHADOW_PREFIX: &str = "pgmg_shadow__";
const SAVEPOINT: &str = "pgmg_shadow_step";

fn shadow_schema(schema: &str) -> String {
    format!("{SHADOW_PREFIX}{schema}")
}

/// The columns each of `views` will have once the plan is applied, as a client
/// describing a query over it is told them, keyed by (schema, name). Views
/// whose shape could not be worked out are absent.
pub async fn view_shapes_after_apply(
    client: &Client,
    migrations: &[&str],
    views: &[&SqlObject],
) -> HashMap<(String, String), Vec<(String, u32)>> {
    if client.batch_execute("BEGIN").await.is_err() {
        return HashMap::new();
    }
    let shapes = build(client, migrations, views).await.unwrap_or_else(|e| {
        debug!("could not build shadow views: {e}");
        HashMap::new()
    });
    if let Err(e) = client.batch_execute("ROLLBACK").await {
        debug!("could not roll back the shadow transaction: {e}");
    }
    shapes
}

/// Run one statement so that its failure leaves the transaction usable.
async fn step(client: &Client, sql: &str) -> bool {
    if client.batch_execute(&format!("SAVEPOINT {SAVEPOINT}")).await.is_err() {
        return false;
    }
    match client.batch_execute(sql).await {
        Ok(()) => client.batch_execute(&format!("RELEASE SAVEPOINT {SAVEPOINT}")).await.is_ok(),
        Err(e) => {
            debug!(statement = sql, "shadow statement failed: {e}");
            let _ = client.batch_execute(&format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}")).await;
            false
        }
    }
}

async fn build(
    client: &Client,
    migrations: &[&str],
    views: &[&SqlObject],
) -> Result<HashMap<(String, String), Vec<(String, u32)>>, tokio_postgres::Error> {
    // (schema, name) of every relation that has a stand-in.
    let mut shadowed: HashSet<(String, String)> = HashSet::new();
    let mut schemas: HashSet<String> = HashSet::new();

    let parsed: Vec<_> = migrations.iter().filter_map(|content| pg_query::parse(content).ok()).collect();
    let statements = || {
        parsed.iter()
            .flat_map(|migration| migration.protobuf.stmts.iter())
            .filter_map(|stmt| stmt.stmt.as_ref()?.node.as_ref())
    };

    // Types the migrations create, so a new column can be of one. They are
    // created under their real names: nothing existing is touched or locked,
    // and the rollback removes them.
    for node in statements() {
        if matches!(
            node,
            NodeEnum::CreateEnumStmt(_) | NodeEnum::CompositeTypeStmt(_) | NodeEnum::CreateDomainStmt(_)
        ) {
            if let Ok(sql) = node.deparse() {
                step(client, &sql).await;
            }
        }
    }

    // Where a migration's unqualified table names land.
    let default_schema: Option<String> = client.query_one("SELECT current_schema()::text", &[]).await?.get(0);
    let schema_of = |relation: &pg_query::protobuf::RangeVar| {
        if relation.schemaname.is_empty() { default_schema.clone() } else { Some(relation.schemaname.clone()) }
    };

    // Tables the migrations create or add columns to, in statement order, so
    // a table created by one statement can gain columns from a later one.
    for node in statements() {
        match node {
            NodeEnum::CreateStmt(create) => {
                let Some(mut table) = stand_in_table(create) else { continue };
                let Some(relation) = table.relation.as_mut() else { continue };
                let Some(schema) = schema_of(relation) else { continue };
                let name = relation.relname.clone();
                // `CREATE TABLE IF NOT EXISTS` over a table that is already
                // there creates nothing; a stand-in would hide the real shape.
                if shadowed.contains(&(schema.clone(), name.clone()))
                    || existing_relation(client, Some(&schema), &name).await?.is_some()
                {
                    continue;
                }
                if !ensure_shadow_schema(client, &mut schemas, &schema).await {
                    return Ok(HashMap::new());
                }
                relation.schemaname = shadow_schema(&schema);
                if let Ok(sql) = NodeEnum::CreateStmt(table).deparse() {
                    if step(client, &sql).await {
                        shadowed.insert((schema, name));
                    }
                }
            }
            NodeEnum::AlterTableStmt(alter) => {
                let Some(add_columns) = add_columns_only(alter) else { continue };
                let Some(relation) = &alter.relation else { continue };

                // A table created earlier in the batch already has its
                // stand-in; an existing one gets an empty copy.
                let created = schema_of(relation)
                    .map(|schema| (schema, relation.relname.clone()))
                    .filter(|key| shadowed.contains(key));
                let (schema, name) = match created {
                    Some(key) => key,
                    None => {
                        let schema = (!relation.schemaname.is_empty()).then_some(relation.schemaname.as_str());
                        let Some((schema, name)) = existing_relation(client, schema, &relation.relname).await? else {
                            continue;
                        };
                        if !ensure_shadow_schema(client, &mut schemas, &schema).await {
                            return Ok(HashMap::new());
                        }
                        if !shadowed.contains(&(schema.clone(), name.clone())) {
                            let copy = format!(
                                "CREATE TABLE {}.{} (LIKE {}.{})",
                                quote_identifier(&shadow_schema(&schema)),
                                quote_identifier(&name),
                                quote_identifier(&schema),
                                quote_identifier(&name),
                            );
                            if !step(client, &copy).await {
                                continue;
                            }
                            shadowed.insert((schema.clone(), name.clone()));
                        }
                        (schema, name)
                    }
                };
                let shadow = shadow_schema(&schema);

                let mut add = add_columns;
                if let Some(relation) = add.relation.as_mut() {
                    relation.schemaname = shadow.clone();
                    relation.relname = name.clone();
                }
                let added = match NodeEnum::AlterTableStmt(add).deparse() {
                    Ok(sql) => step(client, &sql).await,
                    Err(_) => false,
                };
                if !added {
                    // A stand-in missing a column would under-report shapes.
                    step(client, &format!("DROP TABLE {}.{}", quote_identifier(&shadow), quote_identifier(&name))).await;
                    shadowed.remove(&(schema, name));
                }
            }
            _ => {}
        }
    }

    // Views, in whatever order lets each find the ones it reads from.
    for view in views {
        if !ensure_shadow_schema(client, &mut schemas, view_schema(view)).await {
            return Ok(HashMap::new());
        }
    }

    // Unqualified names resolve to a stand-in before the real relation.
    let path: Vec<String> = client.query_one("SELECT current_schemas(false)", &[]).await?.get(0);
    let search_path: Vec<String> = path
        .iter()
        .flat_map(|schema| {
            let shadow = schemas.contains(schema).then(|| quote_identifier(&shadow_schema(schema)));
            shadow.into_iter().chain(std::iter::once(quote_identifier(schema)))
        })
        .collect();
    if !search_path.is_empty() {
        client
            .batch_execute(&format!("SET LOCAL search_path = {}", search_path.join(", ")))
            .await?;
    }

    let mut pending: Vec<&SqlObject> = views.to_vec();
    let mut built: Vec<(String, String)> = Vec::new();
    loop {
        let mut still_pending = Vec::new();
        let mut progressed = false;
        for view in pending {
            let schema = view_schema(view).to_string();
            let name = view.qualified_name.name.clone();
            let created = match shadow_view_ddl(view, &shadowed) {
                Some(ddl) => step(client, &ddl).await,
                None => false,
            };
            if created {
                shadowed.insert((schema.clone(), name.clone()));
                built.push((schema, name));
                progressed = true;
            } else {
                still_pending.push(view);
            }
        }
        pending = still_pending;
        // A view that failed may only have been waiting for one built in
        // this round, so go again until a round builds nothing.
        if pending.is_empty() || !progressed {
            break;
        }
    }

    let mut shapes = HashMap::new();
    for (schema, name) in built {
        let shape = introspect::get_relation_described_shape(client, &shadow_schema(&schema), &name).await?;
        if !shape.is_empty() {
            shapes.insert((schema, name), shape);
        }
    }
    Ok(shapes)
}

/// Create the scratch schema standing in for `schema` unless it exists
/// already. False when it cannot be created: without the right to create
/// schemas nothing else here can work.
async fn ensure_shadow_schema(client: &Client, schemas: &mut HashSet<String>, schema: &str) -> bool {
    !schemas.insert(schema.to_string())
        || step(client, &format!("CREATE SCHEMA {}", quote_identifier(&shadow_schema(schema)))).await
}

/// The (schema, name) of an existing relation as the catalog has it, resolving
/// an unqualified name along the search path.
async fn existing_relation(
    client: &Client,
    schema: Option<&str>,
    name: &str,
) -> Result<Option<(String, String)>, tokio_postgres::Error> {
    let spelled = introspect::quote_qualified_identifier(schema, name);
    let row = client
        .query_opt(
            "SELECT n.nspname, c.relname FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = to_regclass($1)",
            &[&spelled],
        )
        .await?;
    Ok(row.map(|row| (row.get(0), row.get(1))))
}

fn view_schema(view: &SqlObject) -> &str {
    view.qualified_name.schema.as_deref().unwrap_or("public")
}

/// The `ADD COLUMN` part of an `ALTER TABLE`, reduced to names and types, or
/// `None` if the statement adds no columns to a table. Defaults and
/// constraints are dropped: they do not change the column's type, and they can
/// refer to things that have no stand-in.
fn add_columns_only(alter: &pg_query::protobuf::AlterTableStmt) -> Option<pg_query::protobuf::AlterTableStmt> {
    if !matches!(
        PgObjectType::try_from(alter.objtype),
        Ok(PgObjectType::ObjectTable) | Ok(PgObjectType::ObjectForeignTable)
    ) {
        return None;
    }
    let mut out = alter.clone();
    out.objtype = PgObjectType::ObjectTable as i32;
    out.cmds = alter
        .cmds
        .iter()
        .filter_map(|node| {
            let Some(NodeEnum::AlterTableCmd(cmd)) = &node.node else { return None };
            if AlterTableType::try_from(cmd.subtype) != Ok(AlterTableType::AtAddColumn) {
                return None;
            }
            let mut cmd = cmd.clone();
            cmd.missing_ok = true;
            let Some(NodeEnum::ColumnDef(column)) = cmd.def.as_mut().and_then(|def| def.node.as_mut()) else {
                return None;
            };
            strip_to_name_and_type(column);
            Some(pg_query::protobuf::Node { node: Some(NodeEnum::AlterTableCmd(cmd)) })
        })
        .collect();
    (!out.cmds.is_empty()).then_some(out)
}

fn strip_to_name_and_type(column: &mut pg_query::protobuf::ColumnDef) {
    column.constraints.clear();
    column.raw_default = None;
    column.cooked_default = None;
    column.identity_sequence = None;
    column.identity.clear();
    column.generated.clear();
    column.is_not_null = false;
}

/// A `CREATE TABLE` reduced to column names and types (and `LIKE` clauses),
/// or `None` for a table whose columns are not spelled out in the statement
/// (a partition, an inheritance child, a typed table) or that is temporary.
/// Constraints, defaults and partitioning are dropped for the same reason as
/// in [`add_columns_only`].
fn stand_in_table(create: &pg_query::protobuf::CreateStmt) -> Option<pg_query::protobuf::CreateStmt> {
    let relation = create.relation.as_ref()?;
    if !create.inh_relations.is_empty()
        || create.partbound.is_some()
        || create.of_typename.is_some()
        || relation.relpersistence == "t"
    {
        return None;
    }
    let mut out = create.clone();
    out.if_not_exists = false;
    out.partspec = None;
    out.constraints.clear();
    out.options.clear();
    out.tablespacename.clear();
    out.access_method.clear();
    out.table_elts = create
        .table_elts
        .iter()
        .filter_map(|element| match &element.node {
            Some(NodeEnum::ColumnDef(column)) => {
                let mut column = column.clone();
                strip_to_name_and_type(&mut column);
                Some(pg_query::protobuf::Node { node: Some(NodeEnum::ColumnDef(column)) })
            }
            Some(NodeEnum::TableLikeClause(_)) => Some(element.clone()),
            _ => None,
        })
        .collect();
    (!out.table_elts.is_empty()).then_some(out)
}

/// `CREATE VIEW` for a view's stand-in: its new defining query with references
/// to relations that have stand-ins pointed at them.
fn shadow_view_ddl(view: &SqlObject, shadowed: &HashSet<(String, String)>) -> Option<String> {
    let (query, aliases) = super::severity::extract_view_query(&view.ddl_statement)?;
    let query = retarget(&query, shadowed)?;
    let columns = if aliases.is_empty() {
        String::new()
    } else {
        format!(" ({})", aliases.iter().map(|a| quote_identifier(a)).collect::<Vec<_>>().join(", "))
    };
    Some(format!(
        "CREATE VIEW {}.{}{} AS {}",
        quote_identifier(&shadow_schema(view_schema(view))),
        quote_identifier(&view.qualified_name.name),
        columns,
        query,
    ))
}

/// Rewrite every `schema.relation` in `sql` that names a relation with a
/// stand-in so it names the stand-in. Works on tokens, so string literals and
/// comments are never touched.
fn retarget(sql: &str, shadowed: &HashSet<(String, String)>) -> Option<String> {
    let tokens = pg_query::scan(sql).ok()?.tokens;
    let is_dot = |i: usize| tokens.get(i).is_some_and(|t| t.token() == Token::Ascii46);

    let mut out = String::with_capacity(sql.len() + 64);
    let mut copied = 0usize;
    for i in 0..tokens.len() {
        // `schema` must start the dotted name: in `db.schema.relation` or
        // `alias.column.field` the pair means something else.
        if i > 0 && is_dot(i - 1) || !is_dot(i + 1) {
            continue;
        }
        let (Some(schema), Some(relation)) = (
            identifier_text(sql, &tokens[i]),
            tokens.get(i + 2).and_then(|t| identifier_text(sql, t)),
        ) else {
            continue;
        };
        if !shadowed.contains(&(schema.clone(), relation)) {
            continue;
        }
        let start = usize::try_from(tokens[i].start).ok()?;
        let end = usize::try_from(tokens[i].end).ok()?;
        out.push_str(sql.get(copied..start)?);
        out.push_str(&quote_identifier(&shadow_schema(&schema)));
        copied = end;
    }
    out.push_str(sql.get(copied..)?);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(pairs: &[(&str, &str)]) -> HashSet<(String, String)> {
        pairs.iter().map(|(s, n)| (s.to_string(), n.to_string())).collect()
    }

    #[test]
    fn retarget_rewrites_only_qualified_names_of_shadowed_relations() {
        let shadowed = set(&[("core", "listing"), ("public", "seller")]);
        let sql = "SELECT l.id, core.listing.price, 'core.listing' AS note \
                   FROM core.listing l JOIN core.other o ON o.id = l.id JOIN public.seller s ON true";
        let out = retarget(sql, &shadowed).unwrap();
        assert_eq!(
            out,
            "SELECT l.id, \"pgmg_shadow__core\".listing.price, 'core.listing' AS note \
             FROM \"pgmg_shadow__core\".listing l JOIN core.other o ON o.id = l.id JOIN \"pgmg_shadow__public\".seller s ON true"
        );
    }

    #[test]
    fn retarget_leaves_longer_dotted_names_and_unrelated_sql_alone() {
        let shadowed = set(&[("core", "listing")]);
        for sql in [
            "SELECT 1 FROM listing",
            "SELECT x.core.listing FROM t x",
            "SELECT core.listing_count() FROM t",
        ] {
            assert_eq!(retarget(sql, &shadowed).unwrap(), sql);
        }
    }

    #[test]
    fn add_columns_only_keeps_names_and_types() {
        let parsed = pg_query::parse(
            "ALTER TABLE public.seller ADD COLUMN bnpl boolean NOT NULL DEFAULT false, \
             DROP COLUMN legacy, ADD COLUMN ref int REFERENCES other (id)",
        )
        .unwrap();
        let Some(NodeEnum::AlterTableStmt(alter)) =
            parsed.protobuf.stmts[0].stmt.as_ref().and_then(|s| s.node.clone())
        else {
            panic!("not an ALTER TABLE");
        };
        let sql = NodeEnum::AlterTableStmt(add_columns_only(&alter).unwrap()).deparse().unwrap();
        let lower = sql.to_lowercase();
        assert!(lower.contains("bnpl") && lower.contains("ref"), "{sql}");
        assert!(!lower.contains("default") && !lower.contains("references") && !lower.contains("legacy"), "{sql}");
        assert!(!lower.contains("not null"), "{sql}");
    }

    fn stand_in(sql: &str) -> Option<String> {
        let parsed = pg_query::parse(sql).unwrap();
        let Some(NodeEnum::CreateStmt(create)) = parsed.protobuf.stmts[0].stmt.as_ref().and_then(|s| s.node.clone())
        else {
            panic!("not a CREATE TABLE");
        };
        stand_in_table(&create).map(|table| NodeEnum::CreateStmt(table).deparse().unwrap().to_lowercase())
    }

    #[test]
    fn stand_in_table_keeps_names_and_types() {
        let sql = stand_in(
            "CREATE TABLE IF NOT EXISTS core.note (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
             listing_id int NOT NULL REFERENCES listing (id), body text DEFAULT '', \
             CONSTRAINT body_len CHECK (length(body) < 100), UNIQUE (listing_id)) PARTITION BY HASH (id)",
        ).unwrap();
        for kept in ["core.note", "id bigint", "listing_id int", "body text"] {
            assert!(sql.contains(kept), "{kept} missing from {sql}");
        }
        for dropped in ["references", "default", "check", "unique", "primary", "identity", "partition", "not null", "if not exists"] {
            assert!(!sql.contains(dropped), "{dropped} left in {sql}");
        }
    }

    #[test]
    fn stand_in_table_skips_tables_without_their_own_column_list() {
        for sql in [
            "CREATE TABLE note_1 PARTITION OF note FOR VALUES WITH (MODULUS 2, REMAINDER 0)",
            "CREATE TABLE child () INHERITS (parent)",
            "CREATE TABLE typed OF some_type",
            "CREATE TEMP TABLE scratch (id int)",
            "CREATE TABLE empty ()",
        ] {
            assert!(stand_in(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn add_columns_only_ignores_statements_without_added_columns() {
        for sql in ["ALTER TABLE t DROP COLUMN a", "ALTER TYPE t ADD ATTRIBUTE a int", "ALTER TABLE t ADD CONSTRAINT c CHECK (true)"] {
            let parsed = pg_query::parse(sql).unwrap();
            let Some(NodeEnum::AlterTableStmt(alter)) =
                parsed.protobuf.stmts[0].stmt.as_ref().and_then(|s| s.node.clone())
            else {
                panic!("not an ALTER TABLE");
            };
            assert!(add_columns_only(&alter).is_none(), "{sql}");
        }
    }
}
