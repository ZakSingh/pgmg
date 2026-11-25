//! Analyze migration SQL to work out what its ALTER TABLE statements mean for
//! the managed objects that depend on the altered tables, and which enum
//! ADD VALUE statements need to be pre-committed outside a transaction to avoid
//! PostgreSQL's "unsafe use of new value" error.
//!
//! Dropping and recreating a dependent is only needed when PostgreSQL would
//! refuse the ALTER while the dependent exists (e.g. `ALTER COLUMN ... TYPE`
//! on a column a view selects) or when the dependent's stored definition would
//! no longer match what its source file produces on a fresh build. Most ALTER
//! TABLE forms are neither, so each subcommand is classified here and the
//! planner only recreates what the classification calls for.

use std::collections::{BTreeSet, HashMap, HashSet};
use crate::sql::QualifiedIdent;
use pg_query::NodeEnum;
use pg_query::protobuf::{AlterTableType, ObjectType as PgObjectType, Token};
use crate::sql::late_binding::{has_late_bound_body, identifier_text};

/// What a pending migration's ALTER TABLE statements require of the managed
/// objects depending on the altered table. Ordered from least to most
/// disruptive, so the overall impact of several statements is their `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlterImpact {
    /// Dependents are untouched: PostgreSQL never refuses the change because
    /// of them and their definitions keep their meaning (constraints,
    /// defaults, nullability, ownership, storage, trigger/RLS toggles, ...).
    Unaffected,
    /// Columns were added. PostgreSQL never refuses this, but a dependent whose
    /// definition was resolved against the table's column list when it was
    /// created (`SELECT *`, `NATURAL JOIN`, an unqualified reference the new
    /// column now captures or makes ambiguous) would differ from a fresh
    /// build. Only those dependents are recreated; see
    /// [`definition_sensitive_to_added_columns`].
    ColumnsAdded,
    /// Dependents bound to the table are dropped before the migration and
    /// recreated after: the change can be refused while they exist (`ALTER
    /// COLUMN TYPE`, `DROP COLUMN`, `DROP CONSTRAINT` of a constraint
    /// something relies on), or it is a form pgmg does not classify and so
    /// treats conservatively. Routines that only query the table from a string
    /// body are not bound to it and stay in place.
    Rebuild,
}

/// The combined effect of every ALTER TABLE statement targeting one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAlteration {
    /// The impact of everything except `DROP CONSTRAINT`, which cannot be
    /// judged from the SQL alone. Use [`TableAlteration::impact_given`] or
    /// [`TableAlteration::worst_case_impact`] for the whole picture.
    pub impact: AlterImpact,
    /// Names of the columns added by `ADD COLUMN`, as PostgreSQL stores them
    /// (unquoted names folded to lower case).
    pub added_columns: BTreeSet<String>,
    /// Names of the constraints dropped by `DROP CONSTRAINT`. Dropping one
    /// only disturbs dependents if something in the catalog depends on the
    /// constraint itself (a view grouping by a primary key does), which takes
    /// a catalog lookup to know.
    pub dropped_constraints: BTreeSet<String>,
}

impl TableAlteration {
    fn new() -> Self {
        Self {
            impact: AlterImpact::Unaffected,
            added_columns: BTreeSet::new(),
            dropped_constraints: BTreeSet::new(),
        }
    }

    /// Fold another alteration of the same table into this one.
    pub fn merge(&mut self, other: &TableAlteration) {
        self.impact = self.impact.max(other.impact);
        self.added_columns.extend(other.added_columns.iter().cloned());
        self.dropped_constraints.extend(other.dropped_constraints.iter().cloned());
    }

    /// The overall impact, given whether any of the dropped constraints is
    /// one that existing objects depend on.
    pub fn impact_given(&self, dropped_constraint_in_use: bool) -> AlterImpact {
        if dropped_constraint_in_use && !self.dropped_constraints.is_empty() {
            AlterImpact::Rebuild
        } else {
            self.impact
        }
    }

    /// The overall impact when nothing is known about the dropped constraints.
    pub fn worst_case_impact(&self) -> AlterImpact {
        self.impact_given(true)
    }
}

/// Classify one ALTER TABLE subcommand.
///
/// The `Unaffected` list is an allowlist: anything not named here (including
/// subcommands added by future PostgreSQL versions) falls through to
/// `Rebuild`, which is the behaviour pgmg had for every ALTER TABLE before
/// subcommands were classified.
pub(crate) fn classify_subcommand(subtype: AlterTableType) -> AlterImpact {
    use AlterTableType::*;
    match subtype {
        AtAddColumn => AlterImpact::ColumnsAdded,

        // Column properties that are not part of any dependent's definition.
        AtColumnDefault | AtCookedColumnDefault
        | AtDropNotNull | AtSetNotNull
        | AtSetExpression | AtDropExpression
        | AtAddIdentity | AtSetIdentity | AtDropIdentity
        | AtSetStatistics | AtSetOptions | AtResetOptions
        | AtSetStorage | AtSetCompression
        // New constraints and indexes: nothing existing can depend on them.
        | AtAddConstraint | AtAddIndex | AtAddIndexConstraint
        | AtAlterConstraint | AtValidateConstraint
        // Ownership, storage and physical layout.
        | AtChangeOwner
        | AtClusterOn | AtDropCluster
        | AtSetLogged | AtSetUnLogged
        | AtSetAccessMethod | AtSetTableSpace
        | AtSetRelOptions | AtResetRelOptions | AtReplaceRelOptions
        | AtReplicaIdentity | AtDropOids
        // Trigger firing state. Leaving managed triggers in place is what lets
        // `DISABLE TRIGGER <name>` find the trigger it names.
        | AtEnableTrig | AtEnableAlwaysTrig | AtEnableReplicaTrig | AtDisableTrig
        | AtEnableTrigAll | AtDisableTrigAll | AtEnableTrigUser | AtDisableTrigUser
        // Row-level security switches.
        | AtEnableRowSecurity | AtDisableRowSecurity
        | AtForceRowSecurity | AtNoForceRowSecurity => AlterImpact::Unaffected,

        _ => AlterImpact::Rebuild,
    }
}

/// Everything the planner needs to know about a batch of pending migrations,
/// read off one parse of each.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationAnalysis {
    /// Per altered table, what its ALTER TABLE statements require of the
    /// table's dependents. Keyed as the migration spelled the table (schema
    /// `None` when unqualified).
    pub alterations: HashMap<QualifiedIdent, TableAlteration>,
    /// Types, domains and table row types that are renamed, dropped, moved to
    /// another schema or restructured (`ALTER TYPE ... ADD / DROP / ALTER
    /// ATTRIBUTE`), under the name they had before the statement.
    ///
    /// Whatever is bound to one has to be dropped before the migration and
    /// recreated after it: a drop is refused while dependents exist, and after
    /// a rename a dependent keeps pointing at the renamed type where a fresh
    /// build of its file would resolve the name again (to the type's
    /// replacement, when the migration creates one). A table counts because a
    /// routine whose signature uses its row type is bound to it the same way.
    ///
    /// `ALTER TYPE ... ADD VALUE` and `RENAME VALUE` are not included: they
    /// change an enum's labels, not what is bound to it.
    pub changed_types: HashSet<QualifiedIdent>,
    /// Tables that are dropped, renamed, moved, or have a column renamed.
    /// PostgreSQL lets all of these happen under a routine that only queries
    /// the table from a string body, so those routines need re-validating.
    pub reshaped_tables: HashSet<QualifiedIdent>,
    /// Whether any statement renames a table constraint, after which a
    /// constraint name later in the batch may not be the one in the catalog.
    pub renames_constraints: bool,
}

impl MigrationAnalysis {
    pub fn analyze(sql: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed = pg_query::parse(sql)?;
        let mut analysis = Self::default();
        for stmt in &parsed.protobuf.stmts {
            let Some(node) = stmt.stmt.as_ref().and_then(|s| s.node.as_ref()) else { continue };
            analysis.record(node);
        }
        Ok(analysis)
    }

    /// Fold the analysis of a later migration into this one.
    pub fn merge(&mut self, other: MigrationAnalysis) {
        for (table, alteration) in other.alterations {
            self.alterations.entry(table)
                .and_modify(|existing| existing.merge(&alteration))
                .or_insert(alteration);
        }
        self.changed_types.extend(other.changed_types);
        self.reshaped_tables.extend(other.reshaped_tables);
        self.renames_constraints |= other.renames_constraints;
    }

    fn record(&mut self, node: &NodeEnum) {
        let object_type = |object_type: i32| PgObjectType::try_from(object_type).ok();
        let is_type = |t: i32| matches!(object_type(t), Some(PgObjectType::ObjectType | PgObjectType::ObjectDomain));
        let is_table = |t: i32| matches!(object_type(t), Some(PgObjectType::ObjectTable | PgObjectType::ObjectForeignTable));

        match node {
            // ALTER TYPE / DOMAIN x RENAME TO y
            NodeEnum::RenameStmt(rename) if is_type(rename.rename_type) => {
                self.changed_types.extend(rename.object.as_deref().and_then(qualified_name_of));
            }
            // ALTER TABLE x RENAME TO y
            NodeEnum::RenameStmt(rename) if is_table(rename.rename_type) => {
                self.record_replaced_tables(rename.relation.as_ref().map(range_var_name));
            }
            // ALTER TYPE x RENAME ATTRIBUTE a TO b
            NodeEnum::RenameStmt(rename) if is_type(rename.relation_type) => {
                self.changed_types.extend(rename.relation.as_ref().map(range_var_name));
            }
            // ALTER TABLE x RENAME CONSTRAINT a TO b
            NodeEnum::RenameStmt(rename)
                if object_type(rename.rename_type) == Some(PgObjectType::ObjectTabconstraint) =>
            {
                self.renames_constraints = true;
            }
            // ALTER TABLE x RENAME COLUMN a TO b
            NodeEnum::RenameStmt(rename)
                if is_table(rename.relation_type)
                    && object_type(rename.rename_type) == Some(PgObjectType::ObjectColumn) =>
            {
                self.reshaped_tables.extend(rename.relation.as_ref().map(range_var_name));
            }
            // DROP TYPE / DOMAIN x, y
            NodeEnum::DropStmt(drop) if is_type(drop.remove_type) => {
                self.changed_types.extend(drop.objects.iter().filter_map(qualified_name_of));
            }
            // DROP TABLE x, y
            NodeEnum::DropStmt(drop) if is_table(drop.remove_type) => {
                self.record_replaced_tables(drop.objects.iter().filter_map(qualified_name_of));
            }
            // ALTER TYPE / DOMAIN x SET SCHEMA s
            NodeEnum::AlterObjectSchemaStmt(alter) if is_type(alter.object_type) => {
                self.changed_types.extend(alter.object.as_deref().and_then(qualified_name_of));
            }
            // ALTER TABLE x SET SCHEMA s
            NodeEnum::AlterObjectSchemaStmt(alter) if is_table(alter.object_type) => {
                self.record_replaced_tables(alter.relation.as_ref().map(range_var_name));
            }
            // ALTER TYPE x ADD / DROP / ALTER ATTRIBUTE
            NodeEnum::AlterTableStmt(alter) if is_type(alter.objtype) => {
                self.changed_types.extend(alter.relation.as_ref().map(range_var_name));
            }
            NodeEnum::AlterTableStmt(alter) if is_table(alter.objtype) => self.record_alter_table(alter),
            // ALTER INDEX / VIEW / MATERIALIZED VIEW / SEQUENCE share ALTER
            // TABLE's parse node. None of their forms (options, ownership,
            // column defaults, storage) is refused because of a dependent or
            // changes what a dependent is bound to.
            _ => {}
        }
    }

    /// A table that stops existing under its name takes its row type with it.
    fn record_replaced_tables(&mut self, tables: impl IntoIterator<Item = QualifiedIdent>) {
        for table in tables {
            self.changed_types.insert(table.clone());
            self.reshaped_tables.insert(table);
        }
    }

    fn record_alter_table(&mut self, alter: &pg_query::protobuf::AlterTableStmt) {
        let Some(relation) = &alter.relation else { return };
        let entry = self.alterations
            .entry(range_var_name(relation))
            .or_insert_with(TableAlteration::new);

        for cmd in &alter.cmds {
            let Some(NodeEnum::AlterTableCmd(cmd)) = &cmd.node else {
                entry.impact = AlterImpact::Rebuild;
                continue;
            };
            let impact = match AlterTableType::try_from(cmd.subtype) {
                Ok(AlterTableType::AtDropConstraint) if !cmd.name.is_empty() => {
                    // Judged against the catalog by the caller.
                    entry.dropped_constraints.insert(cmd.name.clone());
                    continue;
                }
                Ok(subtype) => classify_subcommand(subtype),
                Err(_) => AlterImpact::Rebuild,
            };
            if impact == AlterImpact::ColumnsAdded {
                // Without the column's name the sensitivity check has nothing
                // to look for, so fall back to rebuilding.
                match cmd.def.as_ref().and_then(|def| def.node.as_ref()) {
                    Some(NodeEnum::ColumnDef(column)) if !column.colname.is_empty() => {
                        entry.added_columns.insert(column.colname.clone());
                        entry.impact = entry.impact.max(impact);
                    }
                    _ => entry.impact = AlterImpact::Rebuild,
                }
            } else {
                entry.impact = entry.impact.max(impact);
            }
        }
    }
}

/// Analyze every ALTER TABLE statement in migration SQL and return, per
/// altered table, what the statements require of the table's dependents.
/// See [`MigrationAnalysis::alterations`].
pub fn analyze_table_alterations(sql: &str) -> Result<HashMap<QualifiedIdent, TableAlteration>, Box<dyn std::error::Error>> {
    Ok(MigrationAnalysis::analyze(sql)?.alterations)
}

/// Find the types, domains and table row types a migration renames, drops,
/// moves to another schema, or restructures. See
/// [`MigrationAnalysis::changed_types`].
pub fn analyze_type_changes(sql: &str) -> Result<HashSet<QualifiedIdent>, Box<dyn std::error::Error>> {
    Ok(MigrationAnalysis::analyze(sql)?.changed_types)
}

fn range_var_name(relation: &pg_query::protobuf::RangeVar) -> QualifiedIdent {
    let schema = (!relation.schemaname.is_empty()).then(|| relation.schemaname.clone());
    QualifiedIdent::new(schema, relation.relname.clone())
}

/// The (optionally schema-qualified) name held by a node that is a type name
/// or a list of name parts.
fn qualified_name_of(node: &pg_query::protobuf::Node) -> Option<QualifiedIdent> {
    let parts: Vec<&str> = match node.node.as_ref()? {
        NodeEnum::TypeName(type_name) => type_name.names.iter().filter_map(string_value).collect(),
        NodeEnum::List(list) => list.items.iter().filter_map(string_value).collect(),
        NodeEnum::String(s) => vec![s.sval.as_str()],
        _ => return None,
    };
    match parts.as_slice() {
        [name] => Some(QualifiedIdent::new(None, name.to_string())),
        [.., schema, name] => Some(QualifiedIdent::new(Some(schema.to_string()), name.to_string())),
        [] => None,
    }
}

fn string_value(node: &pg_query::protobuf::Node) -> Option<&str> {
    match node.node.as_ref()? {
        NodeEnum::String(s) => Some(s.sval.as_str()),
        _ => None,
    }
}

/// Whether adding `added_columns` to a table this object reads could make the
/// object's stored definition differ from what its DDL produces when run
/// afresh, in which case it must be recreated to keep the database matching
/// the source files.
///
/// PostgreSQL resolves a view's (or any parsed-at-creation object's) column
/// references once, when the object is created. A later `ADD COLUMN` therefore
/// leaves the stored definition alone even where re-running the same text
/// would now resolve differently:
///
/// - `*` / `t.*` was expanded to the columns that existed at the time, so the
///   object would not expose the new column;
/// - `TABLE t` is the same expansion spelled without a star;
/// - `NATURAL JOIN` fixed its join columns at the time, so a new common column
///   would change the join condition;
/// - an unqualified reference spelled like a new column may now be captured by
///   it (a correlated subquery binds to the nearest scope) or be ambiguous.
///
/// The check is deliberately a conservative scan of the DDL's tokens rather
/// than a walk of its parse tree: it cannot miss a construct nested somewhere
/// a tree walker forgot to descend into, and a false positive only costs a
/// recreation that pgmg used to perform unconditionally. Anything that cannot
/// be scanned is reported as sensitive.
///
/// Functions and procedures whose body is a string are never sensitive: the
/// body is parsed when it runs, so it always sees the current columns. Only
/// SQL-standard bodies (`BEGIN ATOMIC` / `RETURN expr`) are parsed at creation.
pub fn definition_sensitive_to_added_columns(ddl: &str, added_columns: &BTreeSet<String>) -> bool {
    if has_late_bound_body(ddl) {
        return false;
    }

    let Ok(scanned) = pg_query::scan(ddl) else { return true };

    // Comments carry no meaning; drop them so neighbours are real tokens.
    let tokens: Vec<_> = scanned.tokens.iter()
        .filter(|t| !matches!(t.token(), Token::SqlComment | Token::CComment))
        .collect();

    for (i, tok) in tokens.iter().enumerate() {
        let prev = i.checked_sub(1).map(|p| tokens[p].token());
        let next = tokens.get(i + 1).map(|t| t.token());

        match tok.token() {
            Token::Natural => return true,

            // `TABLE t` is `SELECT * FROM t` without a star to find. The word
            // also appears in DDL that expands nothing; anywhere else it is
            // taken for the query form.
            Token::Table => {
                let before = i.checked_sub(1)
                    .and_then(|p| token_text(ddl, tokens[p]))
                    .map(|text| text.to_lowercase());
                let ddl_keyword = matches!(
                    before.as_deref(),
                    Some("create" | "alter" | "drop" | "temp" | "temporary" | "unlogged" | "foreign"
                        | "global" | "local" | "on" | "returns" | "old" | "new" | "truncate" | "lock")
                );
                if !ddl_keyword {
                    return true;
                }
            }

            Token::Ascii42 => {
                // `t.*` is always an expansion, whatever follows it.
                let qualified_star = prev == Some(Token::Ascii46);
                // `count(*)` and friends: an aggregate's star, not an expansion.
                let aggregate_star = prev == Some(Token::Ascii40) && next == Some(Token::Ascii41);
                // `a * b`: a bare expansion star is never followed by an operand.
                let multiplication = matches!(
                    next,
                    Some(Token::Ident | Token::Iconst | Token::Fconst | Token::Param | Token::Ascii40)
                );
                if qualified_star || !(aggregate_star || multiplication) {
                    return true;
                }
            }

            // U&"..." identifiers would need unicode-unescaping to compare.
            Token::Uident => return true,

            // Literals and operators can never be column references.
            Token::Sconst | Token::Usconst | Token::Bconst | Token::Xconst
            | Token::Iconst | Token::Fconst | Token::Param => {}

            _ => {
                if added_columns.is_empty() || prev == Some(Token::Ascii46) {
                    // Nothing to match, or a qualified reference (`t.col`),
                    // which keeps pointing at the same column.
                    continue;
                }
                let Some(name) = identifier_text(ddl, tok) else { continue };
                if added_columns.contains(&name) {
                    return true;
                }
            }
        }
    }

    false
}

fn token_text<'a>(sql: &'a str, tok: &pg_query::protobuf::ScanToken) -> Option<&'a str> {
    sql.get(usize::try_from(tok.start).ok()?..usize::try_from(tok.end).ok()?)
}

/// Extract `ALTER TYPE ... ADD VALUE` statements from migration SQL and return
/// them rewritten with `IF NOT EXISTS`.
///
/// These statements cannot be used inside a transaction alongside statements that
/// reference the new enum value. By extracting and pre-committing them before the
/// main migration transaction, we avoid PostgreSQL's "unsafe use of new value" error.
///
/// Returns a vec of (original_sql, rewritten_sql) pairs. The original SQL is used
/// to identify which statements to skip during the main migration, and the rewritten
/// SQL (with IF NOT EXISTS) is executed in the pre-commit phase for idempotency.
pub fn extract_enum_add_value_statements(sql: &str) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let parsed = pg_query::parse(sql)?;
    let mut results = Vec::new();

    for stmt in &parsed.protobuf.stmts {
        if let Some(node) = &stmt.stmt {
            if let Some(NodeEnum::AlterEnumStmt(alter_enum)) = &node.node {
                // Only handle ADD VALUE (new_val is non-empty, old_val is empty)
                if !alter_enum.new_val.is_empty() && alter_enum.old_val.is_empty() {
                    // Get the original statement text
                    let original = NodeEnum::AlterEnumStmt(alter_enum.clone()).deparse()?;

                    // Create a modified version with IF NOT EXISTS
                    let mut modified = alter_enum.clone();
                    modified.skip_if_new_val_exists = true;
                    let rewritten = NodeEnum::AlterEnumStmt(modified).deparse()?;

                    results.push((original, rewritten));
                }
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract_altered_tables(sql: &str) -> Result<HashSet<QualifiedIdent>, Box<dyn std::error::Error>> {
        Ok(analyze_table_alterations(sql)?.into_keys().collect())
    }

    #[test]
    fn test_alter_column_type() {
        let sql = r#"ALTER TABLE "order" ALTER COLUMN seller_id TYPE bigint;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(None, "order".to_string())));
    }

    #[test]
    fn test_alter_column_type_with_schema() {
        let sql = r#"ALTER TABLE public."order" ALTER COLUMN seller_id TYPE bigint;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(Some("public".to_string()), "order".to_string())));
    }

    #[test]
    fn test_alter_table_drop_column() {
        let sql = r#"ALTER TABLE users DROP COLUMN old_field;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(None, "users".to_string())));
    }

    #[test]
    fn test_alter_table_add_column() {
        let sql = r#"ALTER TABLE api.users ADD COLUMN new_field text;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
    }

    #[test]
    fn test_multiple_alter_tables() {
        let sql = r#"
            ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,2);
            ALTER TABLE api.users ADD COLUMN status text;
            ALTER TABLE products DROP COLUMN deprecated_field;
        "#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&QualifiedIdent::new(None, "orders".to_string())));
        assert!(tables.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
        assert!(tables.contains(&QualifiedIdent::new(None, "products".to_string())));
    }

    #[test]
    fn test_no_alter_tables() {
        let sql = r#"
            CREATE TABLE new_table (id serial primary key);
            INSERT INTO users (name) VALUES ('test');
        "#;
        let tables = extract_altered_tables(sql).unwrap();

        assert!(tables.is_empty());
    }

    #[test]
    fn test_alter_table_add_constraint() {
        let sql = r#"ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(None, "orders".to_string())));
    }

    // ---- ALTER TABLE classification -------------------------------------

    fn table(name: &str) -> QualifiedIdent {
        match name.split_once('.') {
            Some((schema, name)) => QualifiedIdent::new(Some(schema.to_string()), name.to_string()),
            None => QualifiedIdent::new(None, name.to_string()),
        }
    }

    /// The impact of a migration that alters exactly one table, `t`, when
    /// nothing is known about the constraints it drops.
    fn impact_on_t(sql: &str) -> AlterImpact {
        let tables = analyze_table_alterations(sql).unwrap();
        assert_eq!(tables.len(), 1, "expected only t to be altered by: {sql}");
        tables[&table("t")].worst_case_impact()
    }

    fn columns(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn subcommands_that_leave_dependents_alone() {
        for sql in [
            // Constraints and indexes
            "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)",
            "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NOT VALID",
            "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES u (id)",
            "ALTER TABLE t ADD PRIMARY KEY (id)",
            "ALTER TABLE t ADD UNIQUE (a, b)",
            "ALTER TABLE t ADD CONSTRAINT u UNIQUE USING INDEX t_a_idx",
            "ALTER TABLE t ADD CONSTRAINT x EXCLUDE USING gist (r WITH &&)",
            "ALTER TABLE t VALIDATE CONSTRAINT c",
            "ALTER TABLE t ALTER CONSTRAINT fk DEFERRABLE INITIALLY DEFERRED",
            // Column properties
            "ALTER TABLE t ALTER COLUMN a SET DEFAULT 0",
            "ALTER TABLE t ALTER COLUMN a DROP DEFAULT",
            "ALTER TABLE t ALTER COLUMN a SET NOT NULL",
            "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
            "ALTER TABLE t ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY",
            "ALTER TABLE t ALTER COLUMN a SET GENERATED BY DEFAULT",
            "ALTER TABLE t ALTER COLUMN a DROP IDENTITY IF EXISTS",
            "ALTER TABLE t ALTER COLUMN a DROP EXPRESSION",
            "ALTER TABLE t ALTER COLUMN a SET STATISTICS 500",
            "ALTER TABLE t ALTER COLUMN a SET STORAGE EXTERNAL",
            "ALTER TABLE t ALTER COLUMN a SET COMPRESSION lz4",
            "ALTER TABLE t ALTER COLUMN a SET (n_distinct = 100)",
            "ALTER TABLE t ALTER COLUMN a RESET (n_distinct)",
            // Ownership, storage, physical layout
            "ALTER TABLE t OWNER TO app",
            "ALTER TABLE t SET (fillfactor = 70)",
            "ALTER TABLE t RESET (fillfactor)",
            "ALTER TABLE t SET TABLESPACE fast",
            "ALTER TABLE t SET LOGGED",
            "ALTER TABLE t SET UNLOGGED",
            "ALTER TABLE t CLUSTER ON t_pkey",
            "ALTER TABLE t SET WITHOUT CLUSTER",
            "ALTER TABLE t REPLICA IDENTITY FULL",
            // Trigger firing state
            "ALTER TABLE t DISABLE TRIGGER USER",
            "ALTER TABLE t ENABLE TRIGGER USER",
            "ALTER TABLE t DISABLE TRIGGER ALL",
            "ALTER TABLE t ENABLE TRIGGER ALL",
            "ALTER TABLE t DISABLE TRIGGER audit_t",
            "ALTER TABLE t ENABLE TRIGGER audit_t",
            "ALTER TABLE t ENABLE ALWAYS TRIGGER audit_t",
            "ALTER TABLE t ENABLE REPLICA TRIGGER audit_t",
            // Row-level security
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE t DISABLE ROW LEVEL SECURITY",
            "ALTER TABLE t FORCE ROW LEVEL SECURITY",
            "ALTER TABLE t NO FORCE ROW LEVEL SECURITY",
        ] {
            assert_eq!(impact_on_t(sql), AlterImpact::Unaffected, "{sql}");
        }
    }

    #[test]
    fn subcommands_that_rebuild_dependents() {
        for sql in [
            "ALTER TABLE t ALTER COLUMN a TYPE bigint",
            "ALTER TABLE t ALTER COLUMN a SET DATA TYPE text USING a::text",
            "ALTER TABLE t DROP COLUMN a",
            "ALTER TABLE t DROP COLUMN IF EXISTS a CASCADE",
            "ALTER TABLE t DROP CONSTRAINT c",
            "ALTER TABLE t DROP CONSTRAINT IF EXISTS t_pkey CASCADE",
            // Not classified, so handled the way every ALTER TABLE used to be.
            "ALTER TABLE t INHERIT parent",
            "ALTER TABLE t NO INHERIT parent",
            "ALTER TABLE t ATTACH PARTITION t_1 FOR VALUES IN (1)",
            "ALTER TABLE t DETACH PARTITION t_1",
            "ALTER TABLE t ENABLE RULE r",
            "ALTER TABLE t DISABLE RULE r",
            "ALTER TABLE t OF some_type",
            "ALTER TABLE t NOT OF",
        ] {
            assert_eq!(impact_on_t(sql), AlterImpact::Rebuild, "{sql}");
        }
    }

    #[test]
    fn add_column_reports_the_new_column_names() {
        for (sql, expected) in [
            ("ALTER TABLE t ADD COLUMN score int", vec!["score"]),
            ("ALTER TABLE t ADD score int", vec!["score"]),
            ("ALTER TABLE t ADD COLUMN IF NOT EXISTS score int NOT NULL DEFAULT 0", vec!["score"]),
            ("ALTER TABLE t ADD COLUMN owner_id int REFERENCES u (id) ON DELETE CASCADE", vec!["owner_id"]),
            ("ALTER TABLE t ADD COLUMN total numeric GENERATED ALWAYS AS (a * 2) STORED", vec!["total"]),
            ("ALTER TABLE t ADD COLUMN a int, ADD COLUMN b text", vec!["a", "b"]),
            // Names are reported as PostgreSQL stores them.
            ("ALTER TABLE t ADD COLUMN MixedCase int", vec!["mixedcase"]),
            (r#"ALTER TABLE t ADD COLUMN "MixedCase" int"#, vec!["MixedCase"]),
            (r#"ALTER TABLE t ADD COLUMN "order" int"#, vec!["order"]),
        ] {
            let tables = analyze_table_alterations(sql).unwrap();
            let alteration = &tables[&table("t")];
            assert_eq!(alteration.impact, AlterImpact::ColumnsAdded, "{sql}");
            assert_eq!(alteration.added_columns, columns(&expected), "{sql}");
        }
    }

    #[test]
    fn the_most_disruptive_subcommand_wins() {
        // Within one statement
        let tables = analyze_table_alterations(
            "ALTER TABLE t ADD COLUMN b int, ALTER COLUMN a SET NOT NULL, ADD CONSTRAINT c CHECK (b > 0)",
        ).unwrap();
        assert_eq!(tables[&table("t")].impact, AlterImpact::ColumnsAdded);
        assert_eq!(tables[&table("t")].added_columns, columns(&["b"]));

        let tables = analyze_table_alterations(
            "ALTER TABLE t ADD COLUMN b int, DROP COLUMN a",
        ).unwrap();
        assert_eq!(tables[&table("t")].impact, AlterImpact::Rebuild);

        // Across statements, in either order
        for sql in [
            "ALTER TABLE t ADD COLUMN b int; ALTER TABLE t ALTER COLUMN a TYPE bigint;",
            "ALTER TABLE t ALTER COLUMN a TYPE bigint; ALTER TABLE t ADD COLUMN b int;",
            "ALTER TABLE t ALTER COLUMN a TYPE bigint; ALTER TABLE t OWNER TO app;",
        ] {
            assert_eq!(impact_on_t(sql), AlterImpact::Rebuild, "{sql}");
        }
        assert_eq!(
            impact_on_t("ALTER TABLE t OWNER TO app; ALTER TABLE t ADD COLUMN b int; ALTER TABLE t ENABLE ROW LEVEL SECURITY;"),
            AlterImpact::ColumnsAdded,
        );
    }

    #[test]
    fn tables_are_classified_independently() {
        let tables = analyze_table_alterations(r#"
            CREATE TABLE fresh (id int);
            ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,2);
            ALTER TABLE api.users ADD COLUMN status text;
            ALTER TABLE ONLY products ADD CONSTRAINT positive CHECK (price > 0);
            ALTER TABLE IF EXISTS "Audit Log" DROP COLUMN note;
            UPDATE api.users SET status = 'active';
        "#).unwrap();

        assert_eq!(tables.len(), 4);
        assert_eq!(tables[&table("orders")].impact, AlterImpact::Rebuild);
        assert_eq!(tables[&table("api.users")].impact, AlterImpact::ColumnsAdded);
        assert_eq!(tables[&table("api.users")].added_columns, columns(&["status"]));
        assert_eq!(tables[&table("products")].impact, AlterImpact::Unaffected);
        assert_eq!(tables[&table("Audit Log")].impact, AlterImpact::Rebuild);
        assert!(tables.values().all(|t| t.dropped_constraints.is_empty()));
    }

    #[test]
    fn dropped_constraints_are_reported_for_the_caller_to_judge() {
        let tables = analyze_table_alterations(r#"
            ALTER TABLE t DROP CONSTRAINT t_check;
            ALTER TABLE t DROP CONSTRAINT IF EXISTS "Odd Name" CASCADE, ADD CONSTRAINT t_check CHECK (a > 0);
        "#).unwrap();
        let t = &tables[&table("t")];
        assert_eq!(t.dropped_constraints, columns(&["t_check", "Odd Name"]));

        // Harmless if nothing depends on the constraints, a rebuild if something does.
        assert_eq!(t.impact, AlterImpact::Unaffected);
        assert_eq!(t.impact_given(false), AlterImpact::Unaffected);
        assert_eq!(t.impact_given(true), AlterImpact::Rebuild);
        assert_eq!(t.worst_case_impact(), AlterImpact::Rebuild);

        // Other subcommands keep their own impact either way.
        let tables = analyze_table_alterations("ALTER TABLE t DROP CONSTRAINT c, ADD COLUMN b int").unwrap();
        let t = &tables[&table("t")];
        assert_eq!(t.impact_given(false), AlterImpact::ColumnsAdded);
        assert_eq!(t.impact_given(true), AlterImpact::Rebuild);

        let tables = analyze_table_alterations("ALTER TABLE t DROP CONSTRAINT c, DROP COLUMN b").unwrap();
        assert_eq!(tables[&table("t")].impact_given(false), AlterImpact::Rebuild);

        // A table with no dropped constraints is unaffected by the question.
        let tables = analyze_table_alterations("ALTER TABLE t ADD COLUMN b int").unwrap();
        assert_eq!(tables[&table("t")].impact_given(true), AlterImpact::ColumnsAdded);
    }

    #[test]
    fn merging_alterations_across_migrations() {
        let mut first = analyze_table_alterations("ALTER TABLE t ADD COLUMN a int").unwrap().remove(&table("t")).unwrap();
        let second = analyze_table_alterations("ALTER TABLE t ADD COLUMN b int").unwrap().remove(&table("t")).unwrap();
        let third = analyze_table_alterations("ALTER TABLE t DROP COLUMN a").unwrap().remove(&table("t")).unwrap();

        first.merge(&second);
        assert_eq!(first.impact, AlterImpact::ColumnsAdded);
        assert_eq!(first.added_columns, columns(&["a", "b"]));

        first.merge(&third);
        assert_eq!(first.impact, AlterImpact::Rebuild);

        let fourth = analyze_table_alterations("ALTER TABLE t DROP CONSTRAINT c").unwrap().remove(&table("t")).unwrap();
        first.merge(&fourth);
        assert_eq!(first.dropped_constraints, columns(&["c"]));
    }

    #[test]
    fn non_table_alters_are_not_table_alterations() {
        // These share ALTER TABLE's parse node, but none of their forms
        // disturbs a dependent.
        for sql in [
            "ALTER INDEX t_idx SET (fillfactor = 70)",
            "ALTER VIEW v ALTER COLUMN a SET DEFAULT 1",
            "ALTER VIEW v OWNER TO app",
            "ALTER MATERIALIZED VIEW mv SET (autovacuum_enabled = false)",
            "ALTER SEQUENCE s OWNER TO app",
            // Reported as a type change instead.
            "ALTER TYPE pair ADD ATTRIBUTE z int",
        ] {
            assert!(analyze_table_alterations(sql).unwrap().is_empty(), "{sql}");
        }

        // Foreign tables are tables.
        assert_eq!(impact_on_t("ALTER FOREIGN TABLE t ADD COLUMN b int"), AlterImpact::ColumnsAdded);
        assert_eq!(impact_on_t("ALTER FOREIGN TABLE t OWNER TO app"), AlterImpact::Unaffected);
    }

    #[test]
    fn statements_that_are_not_alter_table_are_ignored() {
        // RENAME and SET SCHEMA have their own parse nodes and were never
        // treated as table alterations.
        let tables = analyze_table_alterations(r#"
            ALTER TABLE t RENAME COLUMN a TO b;
            ALTER TABLE t RENAME TO t2;
            ALTER TABLE t SET SCHEMA archive;
            DROP TABLE gone;
            COMMENT ON TABLE t IS 'x';
        "#).unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn unparseable_migrations_are_an_error() {
        assert!(analyze_table_alterations("ALTER TABLE t ADD COLUMN").is_err());
    }

    // ---- Types changed by a migration ------------------------------------

    fn changed_types(sql: &str) -> Vec<String> {
        let mut names: Vec<String> = analyze_type_changes(sql).unwrap().iter()
            .map(|t| match &t.schema {
                Some(schema) => format!("{schema}.{}", t.name),
                None => t.name.clone(),
            })
            .collect();
        names.sort();
        names
    }

    #[test]
    fn type_renames_drops_and_restructurings_are_reported() {
        for (sql, expected) in [
            ("ALTER TYPE grade RENAME TO grade_old", vec!["grade"]),
            ("ALTER TYPE public.grade RENAME TO grade_old", vec!["public.grade"]),
            (r#"ALTER TYPE "Odd Schema"."Odd Type" RENAME TO plain"#, vec!["Odd Schema.Odd Type"]),
            ("ALTER DOMAIN probability RENAME TO prob", vec!["probability"]),
            ("DROP TYPE grade", vec!["grade"]),
            ("DROP TYPE IF EXISTS api.pair, grade CASCADE", vec!["api.pair", "grade"]),
            ("DROP DOMAIN probability", vec!["probability"]),
            ("ALTER TYPE grade SET SCHEMA archive", vec!["grade"]),
            ("ALTER DOMAIN core.probability SET SCHEMA archive", vec!["core.probability"]),
            ("ALTER TYPE api.pair ADD ATTRIBUTE z int", vec!["api.pair"]),
            ("ALTER TYPE pair DROP ATTRIBUTE b", vec!["pair"]),
            ("ALTER TYPE pair ALTER ATTRIBUTE b TYPE bigint", vec!["pair"]),
            ("ALTER TYPE pair RENAME ATTRIBUTE b TO c", vec!["pair"]),
            // The replace-an-enum pattern reports the name dependents know.
            ("ALTER TYPE grade RENAME TO grade_old; CREATE TYPE grade AS ENUM ('a'); DROP TYPE grade_old;",
             vec!["grade", "grade_old"]),
        ] {
            assert_eq!(changed_types(sql), expected, "{sql}");
        }
    }

    #[test]
    fn statements_that_leave_type_bindings_alone_are_not_reported() {
        for sql in [
            "ALTER TYPE grade ADD VALUE 'best'",
            "ALTER TYPE grade ADD VALUE IF NOT EXISTS 'best' AFTER 'good'",
            "ALTER TYPE grade RENAME VALUE 'good' TO 'fine'",
            "ALTER TYPE grade OWNER TO app",
            "ALTER DOMAIN probability ADD CONSTRAINT upper CHECK (VALUE <= 1)",
            "ALTER DOMAIN probability DROP CONSTRAINT upper",
            "ALTER DOMAIN probability SET DEFAULT 0",
            "CREATE TYPE grade AS ENUM ('a', 'b')",
            "CREATE DOMAIN probability AS real",
            "COMMENT ON TYPE grade IS 'x'",
            // Not types, and the table's row type keeps its name
            "ALTER TABLE t RENAME COLUMN a TO b",
            "ALTER TABLE t RENAME CONSTRAINT a TO b",
            "DROP VIEW grade",
            "DROP FUNCTION grade()",
            "DROP INDEX grade",
            "ALTER TABLE t ADD COLUMN g grade",
            "ALTER TABLE t DROP COLUMN g",
        ] {
            assert!(changed_types(sql).is_empty(), "{sql}: {:?}", changed_types(sql));
        }
    }

    #[test]
    fn tables_that_stop_existing_under_their_name_take_their_row_type_along() {
        for (sql, expected) in [
            ("DROP TABLE listing", vec!["listing"]),
            ("DROP TABLE IF EXISTS core.listing, seller CASCADE", vec!["core.listing", "seller"]),
            ("ALTER TABLE listing RENAME TO listing_old", vec!["listing"]),
            ("ALTER TABLE core.listing SET SCHEMA archive", vec!["core.listing"]),
            ("DROP FOREIGN TABLE remote_listing", vec!["remote_listing"]),
            ("ALTER TABLE listing RENAME TO listing_old; CREATE TABLE listing (id int); DROP TABLE listing_old;",
             vec!["listing", "listing_old"]),
        ] {
            assert_eq!(changed_types(sql), expected, "{sql}");
            let analysis = MigrationAnalysis::analyze(sql).unwrap();
            assert_eq!(analysis.reshaped_tables, analysis.changed_types, "{sql}");
            assert!(analysis.alterations.is_empty(), "{sql}");
        }
    }

    #[test]
    fn renamed_columns_and_constraints_are_noted() {
        let analysis = MigrationAnalysis::analyze("ALTER TABLE api.t RENAME COLUMN a TO b").unwrap();
        assert_eq!(analysis.reshaped_tables, HashSet::from([table("api.t")]));
        assert!(analysis.changed_types.is_empty() && !analysis.renames_constraints);

        let analysis = MigrationAnalysis::analyze("ALTER TABLE t RENAME CONSTRAINT a TO b").unwrap();
        assert!(analysis.renames_constraints);
        assert!(analysis.reshaped_tables.is_empty());

        assert!(!MigrationAnalysis::analyze("ALTER TABLE t DROP CONSTRAINT a").unwrap().renames_constraints);
    }

    #[test]
    fn analyses_of_successive_migrations_merge() {
        let mut all = MigrationAnalysis::analyze("ALTER TABLE t ADD COLUMN a int; DROP TYPE grade;").unwrap();
        all.merge(MigrationAnalysis::analyze(
            "ALTER TABLE t DROP COLUMN b; ALTER TABLE u RENAME COLUMN x TO y; ALTER TABLE u RENAME CONSTRAINT c TO d;",
        ).unwrap());
        assert_eq!(all.alterations[&table("t")].impact, AlterImpact::Rebuild);
        assert_eq!(all.alterations[&table("t")].added_columns, columns(&["a"]));
        assert_eq!(all.changed_types, HashSet::from([table("grade")]));
        assert_eq!(all.reshaped_tables, HashSet::from([table("u")]));
        assert!(all.renames_constraints);
    }

    // ---- Sensitivity of a definition to added columns ---------------------

    #[test]
    fn the_table_command_is_a_star_expansion() {
        for ddl in [
            "CREATE VIEW v AS TABLE t",
            "CREATE VIEW v AS table public.t",
            "CREATE VIEW v AS (TABLE t)",
            "CREATE VIEW v AS SELECT t.id FROM t UNION ALL TABLE u",
            "CREATE VIEW v AS WITH c AS (TABLE t) SELECT c.id FROM c",
            "CREATE MATERIALIZED VIEW mv AS TABLE ONLY t",
            "CREATE FUNCTION f() RETURNS SETOF t LANGUAGE sql BEGIN ATOMIC TABLE t; END",
        ] {
            assert!(sensitive(ddl, &[]), "{ddl}");
        }
        // The word in DDL that expands nothing.
        for ddl in [
            "CREATE TABLE t (id int)",
            "CREATE UNLOGGED TABLE t (id int)",
            "COMMENT ON TABLE t IS 'x'",
            "CREATE FUNCTION f() RETURNS TABLE (id int) LANGUAGE sql BEGIN ATOMIC SELECT t.id FROM t; END",
            "CREATE TRIGGER trg AFTER INSERT ON t REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION f()",
            "CREATE VIEW v AS SELECT t.\"table\" FROM t",
        ] {
            assert!(!sensitive(ddl, &["added"]), "{ddl}");
        }
    }

    fn sensitive(ddl: &str, added: &[&str]) -> bool {
        definition_sensitive_to_added_columns(ddl, &columns(added))
    }

    #[test]
    fn star_expansions_are_sensitive() {
        for ddl in [
            "CREATE VIEW v AS SELECT * FROM t",
            "CREATE VIEW v AS SELECT t.* FROM t",
            "CREATE VIEW v AS SELECT public.t.* FROM public.t",
            "CREATE VIEW v AS SELECT t.id, u.* FROM t JOIN u ON u.t_id = t.id",
            "CREATE VIEW v AS SELECT id, * FROM t",
            "CREATE VIEW v AS SELECT DISTINCT * FROM t",
            "CREATE VIEW v AS SELECT DISTINCT ON (id) * FROM t",
            "CREATE VIEW v AS SELECT *\nFROM t",
            "CREATE VIEW v AS SELECT * -- everything\nFROM t",
            "CREATE VIEW v AS SELECT /* all */ * /* of it */ FROM t",
            // Nested where a tree walker might not look
            "CREATE VIEW v AS SELECT s.id FROM (SELECT * FROM t) s",
            "CREATE VIEW v AS WITH c AS (SELECT * FROM t) SELECT id FROM c",
            "CREATE VIEW v AS WITH RECURSIVE c AS (SELECT * FROM t UNION ALL SELECT t.* FROM t JOIN c ON t.parent = c.id) SELECT id FROM c",
            "CREATE VIEW v AS SELECT id FROM t WHERE EXISTS (SELECT * FROM u WHERE u.id = t.id)",
            "CREATE VIEW v AS SELECT id FROM t UNION ALL SELECT * FROM u",
            "CREATE VIEW v AS SELECT id, (SELECT x FROM (SELECT * FROM u) q LIMIT 1) FROM t",
            "CREATE VIEW v AS SELECT l.* FROM t, LATERAL (SELECT * FROM u WHERE u.id = t.id) l",
            "CREATE VIEW v AS SELECT row_to_json(t.*) FROM t",
            "CREATE VIEW v AS SELECT (t.pair).* FROM t",
            "CREATE VIEW v AS SELECT count(*), t.* FROM t GROUP BY t.id",
            "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t",
            "CREATE OR REPLACE VIEW v AS SELECT * FROM t",
            // SQL-standard function bodies are parsed at creation
            "CREATE FUNCTION f() RETURNS SETOF t LANGUAGE sql BEGIN ATOMIC SELECT * FROM t; END",
            "CREATE FUNCTION f() RETURNS t LANGUAGE sql RETURN (SELECT t.* FROM t LIMIT 1)",
            "CREATE RULE r AS ON INSERT TO t DO ALSO INSERT INTO log SELECT new.*",
        ] {
            assert!(sensitive(ddl, &["added"]), "{ddl}");
            // A star is sensitive whatever the new column is called.
            assert!(sensitive(ddl, &[]), "{ddl}");
        }
    }

    #[test]
    fn stars_that_are_not_expansions_are_not_sensitive() {
        for ddl in [
            "CREATE VIEW v AS SELECT count(*) FROM t",
            "CREATE VIEW v AS SELECT count( * ) AS n FROM t",
            "CREATE VIEW v AS SELECT count(*) FILTER (WHERE t.ok) FROM t",
            "CREATE VIEW v AS SELECT t.id, count(*) OVER (PARTITION BY t.kind) FROM t",
            "CREATE VIEW v AS SELECT t.price * t.qty AS total FROM t",
            "CREATE VIEW v AS SELECT t.price * 2 AS doubled FROM t",
            "CREATE VIEW v AS SELECT t.price * 1.5 AS more FROM t",
            "CREATE VIEW v AS SELECT 2 * t.price AS doubled FROM t",
            "CREATE VIEW v AS SELECT (t.a + t.b) * (t.c - t.d) AS x FROM t",
            "CREATE VIEW v AS SELECT t.price * price_factor(t.kind) AS x FROM t",
            "CREATE VIEW v AS SELECT t.a FROM t WHERE t.note LIKE '%*%'",
            "CREATE VIEW v AS SELECT t.a /* not a * star */ FROM t",
            "CREATE VIEW v AS SELECT t.a -- * nor this\nFROM t",
            "CREATE VIEW v AS SELECT t.a <*> t.b AS x FROM t",
            "CREATE INDEX i ON t ((a * 2))",
        ] {
            assert!(!sensitive(ddl, &["added"]), "{ddl}");
        }
    }

    #[test]
    fn natural_joins_are_sensitive() {
        for ddl in [
            "CREATE VIEW v AS SELECT t.id FROM t NATURAL JOIN u",
            "CREATE VIEW v AS SELECT t.id FROM t natural left join u",
            "CREATE VIEW v AS SELECT q.id FROM (SELECT t.id FROM t NATURAL FULL JOIN u) q",
        ] {
            assert!(sensitive(ddl, &["added"]), "{ddl}");
        }
        // ...but a column or alias that merely contains the word is not.
        assert!(!sensitive("CREATE VIEW v AS SELECT t.natural_key, t.\"natural\" FROM t", &["added"]));
    }

    #[test]
    fn unqualified_uses_of_an_added_name_are_sensitive() {
        for ddl in [
            // Would become ambiguous, or be captured by the nearer scope
            "CREATE VIEW v AS SELECT t.id, status FROM t JOIN u ON u.t_id = t.id",
            "CREATE VIEW v AS SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.code = status)",
            "CREATE VIEW v AS SELECT t.id FROM t JOIN u USING (status)",
            "CREATE VIEW v AS SELECT t.id FROM t ORDER BY status",
            "CREATE VIEW v AS SELECT u.state AS status FROM t JOIN u ON u.t_id = t.id GROUP BY status",
            "CREATE VIEW v AS SELECT upper(Status) FROM t JOIN u ON true",
            "CREATE VIEW v AS SELECT \"status\" FROM t JOIN u ON true",
            "CREATE VIEW v AS SELECT coalesce(u.x, status) FROM t LEFT JOIN u ON true",
            "CREATE MATERIALIZED VIEW mv AS SELECT status, count(*) FROM u GROUP BY 1",
            "CREATE FUNCTION f() RETURNS text LANGUAGE sql RETURN (SELECT status FROM u LIMIT 1)",
        ] {
            assert!(sensitive(ddl, &["status"]), "{ddl}");
            // The same definition is untouched by a column it never mentions.
            assert!(!sensitive(ddl, &["unrelated"]), "{ddl}");
        }
    }

    #[test]
    fn added_names_are_compared_the_way_postgres_folds_identifiers() {
        // Unquoted identifiers fold to lower case; quoted ones are exact.
        assert!(sensitive("CREATE VIEW v AS SELECT STATUS FROM u", &["status"]));
        assert!(!sensitive("CREATE VIEW v AS SELECT \"Status\" FROM u", &["status"]));
        assert!(sensitive("CREATE VIEW v AS SELECT \"Status\" FROM u", &["Status"]));
        assert!(!sensitive("CREATE VIEW v AS SELECT status FROM u", &["Status"]));
        assert!(sensitive("CREATE VIEW v AS SELECT \"a\"\"b\" FROM u", &["a\"b"]));

        // Keywords PostgreSQL accepts as column names are still names...
        assert!(sensitive("CREATE VIEW v AS SELECT name, type, value FROM u", &["name"]));
        assert!(sensitive("CREATE VIEW v AS SELECT name, type, value FROM u", &["type"]));
        assert!(sensitive("CREATE VIEW v AS SELECT name, type, value FROM u", &["value"]));
        // ...while a reserved word is only a column when quoted.
        assert!(!sensitive("CREATE VIEW v AS SELECT u.a FROM u ORDER BY u.a", &["order"]));
        assert!(!sensitive("CREATE VIEW v AS SELECT u.a FROM u GROUP BY u.a", &["group"]));
        assert!(sensitive("CREATE VIEW v AS SELECT \"order\" FROM u", &["order"]));
    }

    #[test]
    fn qualified_references_and_literals_are_not_sensitive() {
        for ddl in [
            "CREATE VIEW v AS SELECT t.id, u.status FROM t JOIN u ON u.t_id = t.id",
            "CREATE VIEW v AS SELECT t.id FROM t WHERE t.kind = 'status'",
            "CREATE VIEW v AS SELECT t.id FROM t WHERE t.kind = $q$status$q$",
            "CREATE VIEW v AS SELECT t.id FROM t -- status\n",
            "CREATE VIEW v AS SELECT t.id /* status */ FROM t",
            "CREATE VIEW v AS SELECT t.id, (t.pair).status FROM t",
            "CREATE VIEW v AS SELECT t.id FROM t JOIN public.u ON public.u.status = t.kind",
            "CREATE VIEW v AS SELECT t.id AS status_code, t.statuses FROM t",
            "CREATE INDEX t_kind_idx ON t (kind)",
            "CREATE TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION f()",
            "COMMENT ON COLUMN t.status IS 'the status column'",
            "COMMENT ON VIEW v IS 'shows status'",
        ] {
            assert!(!sensitive(ddl, &["status"]), "{ddl}");
        }
    }

    #[test]
    fn string_bodied_functions_are_never_sensitive() {
        // The body is parsed when it runs, so it always sees current columns;
        // nothing outside the body is resolved against the table.
        for ddl in [
            "CREATE FUNCTION f() RETURNS SETOF t LANGUAGE sql AS $$ SELECT * FROM t $$",
            "CREATE FUNCTION f(status text) RETURNS int LANGUAGE sql AS $$ SELECT count(*)::int FROM t WHERE t.kind = status $$",
            "CREATE FUNCTION f() RETURNS SETOF t LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY SELECT * FROM t NATURAL JOIN u; END $$",
            "CREATE OR REPLACE FUNCTION f(OUT status text) LANGUAGE plpgsql AS 'BEGIN SELECT t.kind INTO status FROM t; END'",
            "CREATE PROCEDURE p(status text) LANGUAGE plpgsql AS $$ BEGIN UPDATE t SET kind = status; END $$",
            "CREATE FUNCTION f() RETURNS TABLE (status text) LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY SELECT t.kind FROM t; END $$",
        ] {
            assert!(!sensitive(ddl, &["status"]), "{ddl}");
        }
    }

    #[test]
    fn definitions_that_cannot_be_scanned_are_sensitive() {
        assert!(sensitive("CREATE VIEW v AS SELECT 'unterminated FROM t", &["x"]));
        assert!(sensitive("CREATE VIEW v AS SELECT U&\"d\\0061t\\+000061\" FROM t", &["x"]));
    }

    #[test]
    fn test_extract_enum_add_value() {
        let sql = r#"ALTER TYPE "public"."enum_status" ADD VALUE 'active' BEFORE 'inactive';"#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert_eq!(results.len(), 1);
        // The rewritten SQL should contain IF NOT EXISTS
        assert!(results[0].1.to_uppercase().contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_extract_enum_add_value_already_if_not_exists() {
        let sql = r#"ALTER TYPE status ADD VALUE IF NOT EXISTS 'active';"#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].1.to_uppercase().contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_extract_enum_add_value_multiple() {
        let sql = r#"
            ALTER TYPE "public"."enum_status" ADD VALUE 'active' BEFORE 'inactive';
            ALTER TABLE users ADD COLUMN status text;
            ALTER TYPE "public"."enum_role" ADD VALUE 'admin';
        "#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_extract_enum_add_value_none_present() {
        let sql = r#"
            ALTER TABLE users ADD COLUMN status text;
            CREATE TABLE orders (id serial primary key);
        "#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_extract_enum_rename_value_not_extracted() {
        // RENAME VALUE should not be extracted - it's not ADD VALUE
        let sql = r#"ALTER TYPE status RENAME VALUE 'old' TO 'new';"#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert!(results.is_empty());
    }
}
