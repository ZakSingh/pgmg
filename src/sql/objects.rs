use std::path::PathBuf;
use crate::sql::parser::{analyze_statement, Dependencies, QualifiedIdent};
use sha2::{Sha256, Digest};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Table,
    View,
    Function,
    Type,
    Domain,
    Index,
    Trigger,
}

#[derive(Debug, Clone)]
pub struct SqlObject {
    pub object_type: ObjectType,
    pub qualified_name: QualifiedIdent,
    pub ddl_statement: String,
    pub dependencies: Dependencies,
    pub source_file: Option<PathBuf>,
    pub ddl_hash: String,
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
        }
    }
}

/// Identify what kind of SQL object a statement creates, if any
pub fn identify_sql_object(statement: &str) -> Result<Option<SqlObject>, Box<dyn std::error::Error>> {
    // Parse the statement to extract dependencies
    let mut dependencies = analyze_statement(statement)?;
    
    // Determine object type and name from the SQL
    let object_info = extract_object_info(statement)?;
    
    match object_info {
        Some((obj_type, qualified_name)) => {
            // Filter out self-references - an object cannot depend on itself
            dependencies.relations.retain(|rel| rel != &qualified_name);
            dependencies.functions.retain(|func| func != &qualified_name);
            dependencies.types.retain(|typ| typ != &qualified_name);
            
            Ok(Some(SqlObject::new(
                obj_type,
                qualified_name,
                normalize_ddl(statement),
                dependencies,
                None, // Set by caller
            )))
        }
        None => Ok(None), // Not a CREATE statement we track
    }
}

/// Extract object type and qualified name from a SQL statement
fn extract_object_info(statement: &str) -> Result<Option<(ObjectType, QualifiedIdent)>, Box<dyn std::error::Error>> {
    let normalized = statement.trim().to_uppercase();
    
    if normalized.starts_with("CREATE TABLE") || normalized.starts_with("CREATE UNLOGGED TABLE") || 
       normalized.starts_with("CREATE TEMP TABLE") || normalized.starts_with("CREATE TEMPORARY TABLE") {
        extract_table_name(statement).map(|name| Some((ObjectType::Table, name)))
    } else if normalized.starts_with("CREATE OR REPLACE VIEW") || normalized.starts_with("CREATE VIEW") {
        extract_view_name(statement).map(|name| Some((ObjectType::View, name)))
    } else if normalized.starts_with("CREATE OR REPLACE FUNCTION") || normalized.starts_with("CREATE FUNCTION") {
        extract_function_name(statement).map(|name| Some((ObjectType::Function, name)))
    } else if normalized.starts_with("CREATE TYPE") {
        extract_type_name(statement).map(|name| Some((ObjectType::Type, name)))
    } else if normalized.starts_with("CREATE DOMAIN") {
        extract_domain_name(statement).map(|name| Some((ObjectType::Domain, name)))
    } else if normalized.starts_with("CREATE INDEX") || normalized.starts_with("CREATE UNIQUE INDEX") {
        extract_index_name(statement).map(|name| Some((ObjectType::Index, name)))
    } else if normalized.starts_with("CREATE TRIGGER") || normalized.starts_with("CREATE OR REPLACE TRIGGER") {
        extract_trigger_name(statement).map(|name| Some((ObjectType::Trigger, name)))
    } else {
        Ok(None)
    }
}

/// Extract table name from CREATE TABLE statement
fn extract_table_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    // Use pg_query to parse and extract the table name
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(pg_query::NodeEnum::CreateStmt(create_stmt)) = &stmt.node {
                if let Some(relation) = &create_stmt.relation {
                    let name = if relation.schemaname.is_empty() {
                        QualifiedIdent::from_name(relation.relname.clone())
                    } else {
                        QualifiedIdent::new(Some(relation.schemaname.clone()), relation.relname.clone())
                    };
                    return Ok(name);
                }
            }
        }
    }
    
    Err("Could not extract table name".into())
}

/// Extract view name from CREATE VIEW statement
fn extract_view_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    // Use pg_query to parse and extract the view name
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(pg_query::NodeEnum::ViewStmt(view_stmt)) = &stmt.node {
                if let Some(range_var) = &view_stmt.view {
                    let name = if range_var.schemaname.is_empty() {
                        QualifiedIdent::from_name(range_var.relname.clone())
                    } else {
                        QualifiedIdent::new(Some(range_var.schemaname.clone()), range_var.relname.clone())
                    };
                    return Ok(name);
                }
            }
        }
    }
    
    Err("Could not extract view name".into())
}

/// Extract function name from CREATE FUNCTION statement
fn extract_function_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(pg_query::NodeEnum::CreateFunctionStmt(func_stmt)) = &stmt.node {
                if !func_stmt.funcname.is_empty() {
                    let name_parts: Vec<String> = func_stmt.funcname.iter()
                        .filter_map(|node| {
                            if let Some(pg_query::NodeEnum::String(string_val)) = &node.node {
                                Some(string_val.sval.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    
                    let name = match name_parts.len() {
                        1 => QualifiedIdent::from_name(name_parts[0].clone()),
                        2 => QualifiedIdent::new(Some(name_parts[0].clone()), name_parts[1].clone()),
                        _ if name_parts.len() > 2 => {
                            let len = name_parts.len();
                            QualifiedIdent::new(
                                Some(name_parts[len - 2].clone()),
                                name_parts[len - 1].clone(),
                            )
                        }
                        _ => return Err("Invalid function name".into()),
                    };
                    return Ok(name);
                }
            }
        }
    }
    
    Err("Could not extract function name".into())
}

/// Extract type name from CREATE TYPE statement
fn extract_type_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            match &stmt.node {
                Some(pg_query::NodeEnum::CompositeTypeStmt(type_stmt)) => {
                    if let Some(type_name) = &type_stmt.typevar {
                        let name = if type_name.schemaname.is_empty() {
                            QualifiedIdent::from_name(type_name.relname.clone())
                        } else {
                            QualifiedIdent::new(Some(type_name.schemaname.clone()), type_name.relname.clone())
                        };
                        return Ok(name);
                    }
                }
                Some(pg_query::NodeEnum::CreateEnumStmt(enum_stmt)) => {
                    if !enum_stmt.type_name.is_empty() {
                        let name_parts: Vec<String> = enum_stmt.type_name.iter()
                            .filter_map(|node| {
                                if let Some(pg_query::NodeEnum::String(string_val)) = &node.node {
                                    Some(string_val.sval.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        
                        let name = match name_parts.len() {
                            1 => QualifiedIdent::from_name(name_parts[0].clone()),
                            2 => QualifiedIdent::new(Some(name_parts[0].clone()), name_parts[1].clone()),
                            _ => return Err("Invalid type name".into()),
                        };
                        return Ok(name);
                    }
                }
                _ => {}
            }
        }
    }
    
    Err("Could not extract type name".into())
}

/// Extract domain name from CREATE DOMAIN statement
fn extract_domain_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(pg_query::NodeEnum::CreateDomainStmt(domain_stmt)) = &stmt.node {
                if !domain_stmt.domainname.is_empty() {
                    let name_parts: Vec<String> = domain_stmt.domainname.iter()
                        .filter_map(|node| {
                            if let Some(pg_query::NodeEnum::String(string_val)) = &node.node {
                                Some(string_val.sval.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    
                    let name = match name_parts.len() {
                        1 => QualifiedIdent::from_name(name_parts[0].clone()),
                        2 => QualifiedIdent::new(Some(name_parts[0].clone()), name_parts[1].clone()),
                        _ => return Err("Invalid domain name".into()),
                    };
                    return Ok(name);
                }
            }
        }
    }
    
    Err("Could not extract domain name".into())
}

/// Extract index name from CREATE INDEX statement
fn extract_index_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(pg_query::NodeEnum::IndexStmt(index_stmt)) = &stmt.node {
                let name = if index_stmt.idxname.is_empty() {
                    // Generate name based on table if no explicit name
                    return Err("Index name not specified".into());
                } else {
                    QualifiedIdent::from_name(index_stmt.idxname.clone())
                };
                return Ok(name);
            }
        }
    }
    
    Err("Could not extract index name".into())
}

/// Extract trigger name from CREATE TRIGGER statement
fn extract_trigger_name(statement: &str) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(statement)?;
    
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(pg_query::NodeEnum::CreateTrigStmt(trigger_stmt)) = &stmt.node {
                let name = QualifiedIdent::from_name(trigger_stmt.trigname.clone());
                return Ok(name);
            }
        }
    }
    
    Err("Could not extract trigger name".into())
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
}