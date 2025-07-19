use std::path::PathBuf;
use std::fmt;
use crate::sql::parser::{Dependencies, QualifiedIdent};
use sha2::{Sha256, Digest};
use pg_query;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Table,
    View,
    MaterializedView,
    Function,
    Type,
    Domain,
    Index,
    Trigger,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectType::Table => write!(f, "TABLE"),
            ObjectType::View => write!(f, "VIEW"),
            ObjectType::MaterializedView => write!(f, "MATERIALIZED VIEW"),
            ObjectType::Function => write!(f, "FUNCTION"),
            ObjectType::Type => write!(f, "TYPE"),
            ObjectType::Domain => write!(f, "DOMAIN"),
            ObjectType::Index => write!(f, "INDEX"),
            ObjectType::Trigger => write!(f, "TRIGGER"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlObject {
    pub object_type: ObjectType,
    pub qualified_name: QualifiedIdent,
    pub ddl_statement: String,
    pub dependencies: Dependencies,
    pub source_file: Option<PathBuf>,
    pub ddl_hash: String,
    pub start_line: Option<usize>,
    pub end_line: Option<usize>,
}

/// Intermediate structure that holds both parsed AST and extracted metadata
/// This ensures we parse each SQL statement exactly once
#[derive(Debug)]
pub struct ParsedSqlObject {
    pub statement: String,
    pub parsed: pg_query::ParseResult,
    pub object_type: ObjectType,
    pub qualified_name: QualifiedIdent,
    pub dependencies: Dependencies,
    pub trigger_table: Option<QualifiedIdent>,
}

impl SqlObject {
    pub fn new(
        object_type: ObjectType,
        qualified_name: QualifiedIdent,
        ddl_statement: String,
        dependencies: Dependencies,
        source_file: Option<PathBuf>,
    ) -> Self {
        let ddl_hash = calculate_ddl_hash(&ddl_statement);
        Self {
            object_type,
            qualified_name,
            ddl_statement,
            dependencies,
            source_file,
            ddl_hash,
            start_line: None,
            end_line: None,
        }
    }
    
    pub fn with_line_numbers(mut self, start_line: Option<usize>, end_line: Option<usize>) -> Self {
        self.start_line = start_line;
        self.end_line = end_line;
        self
    }
}

/// Parse a SQL statement once and extract all necessary information
pub fn parse_sql_object(statement: &str) -> Result<Option<ParsedSqlObject>, Box<dyn std::error::Error>> {
    // Parse the statement once
    let parsed = pg_query::parse(statement)?;
    
    // Check if this is a DDL statement we care about
    for stmt in &parsed.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(node) = &stmt.node {
                match node {
                    pg_query::NodeEnum::CreateStmt(create_table) => {
                        let qualified_name = extract_range_var_name(&create_table.relation)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Table,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::ViewStmt(view_stmt) => {
                        let qualified_name = extract_range_var_name(&view_stmt.view)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        let object_type = if view_stmt.replace {
                            ObjectType::View
                        } else {
                            ObjectType::View
                        };
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::CreateTableAsStmt(ctas) => {
                        if let Some(into) = &ctas.into {
                            // Check if it's a materialized view by looking at the object type
                            if ctas.objtype == 24 { // OBJECT_MATVIEW = 24 in pg_query protobuf
                                let qualified_name = extract_into_clause_name(into)?;
                                let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                                
                                return Ok(Some(ParsedSqlObject {
                                    statement: statement.to_string(),
                                    parsed,
                                    object_type: ObjectType::MaterializedView,
                                    qualified_name,
                                    dependencies,
                                    trigger_table: None,
                                }));
                            }
                        }
                    }
                    pg_query::NodeEnum::CreateFunctionStmt(func_stmt) => {
                        let qualified_name = extract_function_name_from_list(&func_stmt.funcname)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Function,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::CompositeTypeStmt(type_stmt) => {
                        let qualified_name = extract_range_var_name(&type_stmt.typevar)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Type,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::CreateEnumStmt(enum_stmt) => {
                        let qualified_name = extract_name_from_list(&enum_stmt.type_name)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Type,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::CreateDomainStmt(domain_stmt) => {
                        let qualified_name = extract_name_from_list(&domain_stmt.domainname)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Domain,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::IndexStmt(index_stmt) => {
                        let qualified_name = if !index_stmt.idxname.is_empty() {
                            QualifiedIdent::from_name(index_stmt.idxname.clone())
                        } else {
                            return Err("Index has no name".into());
                        };
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Index,
                            qualified_name,
                            dependencies,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::CreateTrigStmt(trigger_stmt) => {
                        let qualified_name = QualifiedIdent::from_name(trigger_stmt.trigname.clone());
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        let trigger_table = trigger_stmt.relation.as_ref()
                            .map(|relation| extract_range_var_name(&Some(relation.clone())))
                            .transpose()?;
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Trigger,
                            qualified_name,
                            dependencies,
                            trigger_table,
                        }));
                    }
                    _ => {}
                }
            }
        }
    }
    
    // Not a DDL statement we care about
    Ok(None)
}

/// Extract qualified name from a RangeVar
fn extract_range_var_name(range_var: &Option<pg_query::protobuf::RangeVar>) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    if let Some(rv) = range_var {
        let name = if !rv.schemaname.is_empty() {
            QualifiedIdent::new(Some(rv.schemaname.clone()), rv.relname.clone())
        } else {
            QualifiedIdent::from_name(rv.relname.clone())
        };
        Ok(name)
    } else {
        Err("No range var provided".into())
    }
}

/// Extract qualified name from an IntoClause (for materialized views)
fn extract_into_clause_name(into: &pg_query::protobuf::IntoClause) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    extract_range_var_name(&into.rel)
}

/// Extract name from a list of nodes (used for functions, types, etc)
fn extract_name_from_list(names: &[pg_query::protobuf::Node]) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    extract_function_name_from_list(names)
}

/// Extract function name from a list of nodes
fn extract_function_name_from_list(names: &[pg_query::protobuf::Node]) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    let parts: Vec<String> = names.iter()
        .filter_map(|node| {
            if let Some(pg_query::NodeEnum::String(s)) = &node.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    match parts.len() {
        1 => Ok(QualifiedIdent::from_name(parts[0].clone())),
        2 => Ok(QualifiedIdent::new(Some(parts[0].clone()), parts[1].clone())),
        _ => Err("Invalid name structure".into())
    }
}

/// Extract dependencies from an already-parsed statement with original SQL for PL/pgSQL analysis
fn extract_dependencies_from_parsed_with_sql(parsed: &pg_query::ParseResult, original_sql: &str) -> Result<Dependencies, Box<dyn std::error::Error>> {
    use crate::sql::parser::extract_dependencies_from_parse_result_with_sql;
    extract_dependencies_from_parse_result_with_sql(&parsed.protobuf, Some(original_sql))
}

/// Identify what kind of SQL object a statement creates, if any
pub fn identify_sql_object(statement: &str) -> Result<Option<SqlObject>, Box<dyn std::error::Error>> {
    // Use the new parse_sql_object function that parses only once
    match parse_sql_object(statement)? {
        Some(parsed_obj) => {
            let mut dependencies = parsed_obj.dependencies;
            
            // Filter out self-references - an object cannot depend on itself
            // Exception: Triggers can depend on functions with the same name
            dependencies.relations.retain(|rel| rel != &parsed_obj.qualified_name);
            if parsed_obj.object_type != ObjectType::Trigger {
                dependencies.functions.retain(|func| func != &parsed_obj.qualified_name);
            }
            dependencies.types.retain(|typ| typ != &parsed_obj.qualified_name);
            
            Ok(Some(SqlObject::new(
                parsed_obj.object_type,
                parsed_obj.qualified_name,
                normalize_ddl(statement),
                dependencies,
                None, // Set by caller
            )))
        }
        None => Ok(None), // Not a CREATE statement we track
    }
}

/// Extract trigger table from CREATE TRIGGER statement
pub fn extract_trigger_table(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    // Use the new parse_sql_object function
    match parse_sql_object(statement)? {
        Some(parsed_obj) => {
            if parsed_obj.object_type == ObjectType::Trigger {
                if let Some(table) = parsed_obj.trigger_table {
                    Ok(table)
                } else {
                    Err("Trigger has no table information".into())
                }
            } else {
                Err("Statement is not a trigger".into())
            }
        }
        None => Err("Could not parse statement".into()),
    }
}

/// Normalize DDL statement for consistent processing
fn normalize_ddl(ddl: &str) -> String {
    // Remove extra whitespace and normalize formatting
    ddl.lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Calculate hash for DDL statement for change detection
pub fn calculate_ddl_hash(ddl: &str) -> String {
    let normalized = normalize_ddl_for_hashing(ddl);
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Normalize DDL for consistent hashing across formatting changes
fn normalize_ddl_for_hashing(ddl: &str) -> String {
    // Remove comments, normalize whitespace, case, etc.
    // for consistent hashing across formatting changes
    ddl.lines()
        .map(|line| line.split("--").next().unwrap_or("").trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identify_create_table() {
        let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Table);
        assert_eq!(obj.qualified_name.name, "users");
        assert!(obj.qualified_name.schema.is_none());
    }

    #[test]
    fn test_identify_qualified_table() {
        let sql = "CREATE TABLE schema1.orders (id UUID PRIMARY KEY, total DECIMAL)";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Table);
        assert_eq!(obj.qualified_name.name, "orders");
        assert_eq!(obj.qualified_name.schema, Some("schema1".to_string()));
    }

    #[test]
    fn test_identify_create_view() {
        let sql = "CREATE VIEW user_stats AS SELECT COUNT(*) FROM users";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::View);
        assert_eq!(obj.qualified_name.name, "user_stats");
        assert!(obj.qualified_name.schema.is_none());
    }

    #[test]
    fn test_identify_qualified_view() {
        let sql = "CREATE OR REPLACE VIEW api.user_stats AS SELECT COUNT(*) FROM api.users";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::View);
        assert_eq!(obj.qualified_name.name, "user_stats");
        assert_eq!(obj.qualified_name.schema, Some("api".to_string()));
    }

    #[test]
    fn test_identify_create_function() {
        let sql = r#"
        CREATE FUNCTION calculate_total(base DECIMAL, tax DECIMAL) 
        RETURNS DECIMAL AS $$
            SELECT base + tax
        $$ LANGUAGE sql;
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Function);
        assert_eq!(obj.qualified_name.name, "calculate_total");
    }

    #[test]
    fn test_identify_create_type() {
        let sql = r#"
        CREATE TYPE address AS (
            street TEXT,
            city TEXT,
            zip_code TEXT
        );
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Type);
        assert_eq!(obj.qualified_name.name, "address");
    }

    #[test]
    fn test_identify_select_statement() {
        let sql = "SELECT * FROM users WHERE active = true";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_none()); // Not a CREATE statement we track
    }

    #[test]
    fn test_ddl_hash_consistency() {
        let sql1 = "CREATE VIEW test AS SELECT 1";
        let sql2 = "  CREATE VIEW test AS\n    SELECT 1  ";
        
        let hash1 = calculate_ddl_hash(sql1);
        let hash2 = calculate_ddl_hash(sql2);
        
        assert_eq!(hash1, hash2); // Should be same despite formatting differences
    }

    #[test]
    fn test_ddl_hash_different_content() {
        let sql1 = "CREATE VIEW test AS SELECT 1";
        let sql2 = "CREATE VIEW test AS SELECT 2";
        
        let hash1 = calculate_ddl_hash(sql1);
        let hash2 = calculate_ddl_hash(sql2);
        
        assert_ne!(hash1, hash2); // Should be different for different content
    }
    
    #[test]
    fn test_identify_with_leading_comment() {
        let sql = r#"-- This is a comment
        CREATE TYPE api.order_item AS (
            id INT,
            name TEXT
        );"#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Type);
        assert_eq!(obj.qualified_name.name, "order_item");
        assert_eq!(obj.qualified_name.schema, Some("api".to_string()));
    }
    
    #[test]
    fn test_identify_with_multiple_comments() {
        let sql = r#"-- First comment
        -- Second comment
        CREATE VIEW api.test_view AS SELECT 1;"#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::View);
        assert_eq!(obj.qualified_name.name, "test_view");
        assert_eq!(obj.qualified_name.schema, Some("api".to_string()));
    }
    
    #[test]
    fn test_identify_create_materialized_view() {
        let sql = r#"
        CREATE MATERIALIZED VIEW product_summary AS
        SELECT p.id, p.name, COUNT(o.id) as order_count
        FROM products p
        LEFT JOIN orders o ON p.id = o.product_id
        GROUP BY p.id, p.name;
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::MaterializedView);
        assert_eq!(obj.qualified_name.name, "product_summary");
        assert!(obj.qualified_name.schema.is_none());
    }
    
    #[test]
    fn test_identify_qualified_materialized_view() {
        let sql = r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS api.product_search_index AS
        SELECT p.product_id,
               to_tsvector('english', p.name) as name_vector
        FROM product p;
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::MaterializedView);
        assert_eq!(obj.qualified_name.name, "product_search_index");
        assert_eq!(obj.qualified_name.schema, Some("api".to_string()));
    }
    
    #[test]
    fn test_identify_materialized_view_with_leading_comment() {
        let sql = r#"-- Create a materialized view for search performance
        CREATE MATERIALIZED VIEW search_cache AS
        SELECT * FROM large_table WHERE active = true;
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::MaterializedView);
        assert_eq!(obj.qualified_name.name, "search_cache");
        assert!(obj.qualified_name.schema.is_none());
    }
    
    #[test]
    fn test_identify_function_with_block_comment() {
        let sql = r#"/*
        This is a multi-line block comment
        explaining the function
        */
        CREATE OR REPLACE FUNCTION process_shipment_cancellation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN NEW;
        END;
        $$;"#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Function);
        assert_eq!(obj.qualified_name.name, "process_shipment_cancellation");
    }
}