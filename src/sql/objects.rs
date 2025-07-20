use std::path::PathBuf;
use std::fmt;
use crate::sql::parser::{Dependencies, QualifiedIdent};
use sha2::{Sha256, Digest};
use pg_query;

/// Operations that can be performed on cron jobs
#[derive(Debug)]
enum CronOperation {
    Schedule { job_name: String, command: String },
    #[allow(dead_code)]
    Unschedule { job_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Table,
    View,
    MaterializedView,
    Function,
    Procedure,
    Type,
    Domain,
    Index,
    Trigger,
    Comment,
    CronJob,
    Aggregate,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectType::Table => write!(f, "TABLE"),
            ObjectType::View => write!(f, "VIEW"),
            ObjectType::MaterializedView => write!(f, "MATERIALIZED VIEW"),
            ObjectType::Function => write!(f, "FUNCTION"),
            ObjectType::Procedure => write!(f, "PROCEDURE"),
            ObjectType::Type => write!(f, "TYPE"),
            ObjectType::Domain => write!(f, "DOMAIN"),
            ObjectType::Index => write!(f, "INDEX"),
            ObjectType::Trigger => write!(f, "TRIGGER"),
            ObjectType::Comment => write!(f, "COMMENT"),
            ObjectType::CronJob => write!(f, "CRON JOB"),
            ObjectType::Aggregate => write!(f, "AGGREGATE"),
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
                        
                        // Check if this is actually a procedure (is_procedure field)
                        let object_type = if func_stmt.is_procedure {
                            ObjectType::Procedure
                        } else {
                            ObjectType::Function
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
                    pg_query::NodeEnum::CommentStmt(comment_stmt) => {
                        // Parse COMMENT ON statements
                        let (qualified_name, comment_dependencies) = parse_comment_target(comment_stmt)?;
                        let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                        
                        // Merge comment target dependencies with statement dependencies
                        let mut merged_deps = dependencies;
                        for rel in comment_dependencies.relations {
                            merged_deps.relations.insert(rel);
                        }
                        for func in comment_dependencies.functions {
                            merged_deps.functions.insert(func);
                        }
                        for typ in comment_dependencies.types {
                            merged_deps.types.insert(typ);
                        }
                        
                        return Ok(Some(ParsedSqlObject {
                            statement: statement.to_string(),
                            parsed,
                            object_type: ObjectType::Comment,
                            qualified_name,
                            dependencies: merged_deps,
                            trigger_table: None,
                        }));
                    }
                    pg_query::NodeEnum::SelectStmt(_) => {
                        // Check if this is a cron.schedule() or cron.unschedule() call
                        if let Some(cron_info) = parse_cron_call_from_statement(statement)? {
                            match cron_info {
                                CronOperation::Schedule { job_name, command } => {
                                    // Get base dependencies (includes cron.schedule)
                                    let mut dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                                    
                                    // Parse command dependencies
                                    let command_deps = parse_cron_command_dependencies(&command);
                                    
                                    // Merge command dependencies
                                    dependencies.relations.extend(command_deps.relations);
                                    dependencies.functions.extend(command_deps.functions);
                                    dependencies.types.extend(command_deps.types);
                                    
                                    // Remove cron.schedule from dependencies (it's not a real dependency)
                                    dependencies.functions.remove(&QualifiedIdent::new(Some("cron".to_string()), "schedule".to_string()));
                                    
                                    return Ok(Some(ParsedSqlObject {
                                        statement: statement.to_string(),
                                        parsed,
                                        object_type: ObjectType::CronJob,
                                        qualified_name: QualifiedIdent::from_name(job_name),
                                        dependencies,
                                        trigger_table: None,
                                    }));
                                }
                                CronOperation::Unschedule { job_name: _ } => {
                                    // For unschedule, we don't create a parsed object
                                    // This will be handled by the drop detection logic
                                    return Ok(None);
                                }
                            }
                        }
                    }
                    pg_query::NodeEnum::DefineStmt(define_stmt) => {
                        // Handle CREATE AGGREGATE statements
                        if define_stmt.kind == 2 { // OBJECT_AGGREGATE = 2 in pg_query protobuf
                            if !define_stmt.defnames.is_empty() {
                                let qualified_name = extract_defname(&define_stmt.defnames)?;
                                let dependencies = extract_dependencies_from_parsed_with_sql(&parsed, statement)?;
                                
                                return Ok(Some(ParsedSqlObject {
                                    statement: statement.to_string(),
                                    parsed,
                                    object_type: ObjectType::Aggregate,
                                    qualified_name,
                                    dependencies,
                                    trigger_table: None,
                                }));
                            }
                        }
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

/// Extract qualified name from a DefineStmt defname list
fn extract_defname(defname: &[pg_query::protobuf::Node]) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    extract_function_name_from_list(defname)
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

/// Extract function signature from CREATE FUNCTION statement
pub fn extract_function_signature(statement: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Parse the DDL to extract the full function signature
    let parsed = pg_query::parse(statement)?;
    
    for stmt in &parsed.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(node) = &stmt.node {
                if let pg_query::NodeEnum::CreateFunctionStmt(func_stmt) = node {
                    // Extract function name
                    let qualified_name = extract_function_name_from_list(&func_stmt.funcname)?;
                    let full_name = format_qualified_name(&qualified_name);
                    
                    // Extract parameter types
                    let param_types: Vec<String> = func_stmt.parameters.iter()
                        .filter_map(|param| {
                            if let Some(pg_query::NodeEnum::FunctionParameter(fp)) = &param.node {
                                // Only include input parameters (exclude OUT parameters)
                                if fp.mode() != pg_query::protobuf::FunctionParameterMode::FuncParamOut &&
                                   fp.mode() != pg_query::protobuf::FunctionParameterMode::FuncParamTable {
                                    if let Some(arg_type) = &fp.arg_type {
                                        return extract_type_name(arg_type);
                                    }
                                }
                            }
                            None
                        })
                        .collect();
                    
                    // Return full signature
                    if param_types.is_empty() {
                        return Ok(format!("{}()", full_name));
                    } else {
                        return Ok(format!("{}({})", full_name, param_types.join(", ")));
                    }
                }
            }
        }
    }
    
    Err("Could not extract function signature from statement".into())
}

/// Helper to extract type name from TypeName node
fn extract_type_name(type_name: &pg_query::protobuf::TypeName) -> Option<String> {
    // Extract the type name from the names list
    let parts: Vec<String> = type_name.names.iter()
        .filter_map(|node| {
            if let Some(pg_query::NodeEnum::String(s)) = &node.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    if parts.is_empty() {
        return None;
    }
    
    // Join parts with dots for qualified types
    let base_type = parts.join(".");
    
    // Check if it's an array type
    if type_name.array_bounds.len() > 0 {
        Some(format!("{}[]", base_type))
    } else {
        Some(base_type)
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

/// Parse a COMMENT statement to extract target object and dependencies
fn parse_comment_target(comment_stmt: &pg_query::protobuf::CommentStmt) -> Result<(QualifiedIdent, Dependencies), Box<dyn std::error::Error>> {
    use pg_query::protobuf::ObjectType as PgObjectType;
    
    let mut dependencies = Dependencies::default();
    
    match comment_stmt.objtype() {
        PgObjectType::ObjectTable => {
            // COMMENT ON TABLE schema.table_name
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::List(list) = node {
                        let qualified_name = extract_name_from_node_list(&list.items)?;
                        // Table comments depend on the table
                        dependencies.relations.insert(qualified_name.clone());
                        let comment_id = QualifiedIdent::new(
                            None,
                            format!("table:{}", format_qualified_name(&qualified_name))
                        );
                        return Ok((comment_id, dependencies));
                    }
                }
            }
        }
        PgObjectType::ObjectColumn => {
            // COMMENT ON COLUMN schema.table.column OR schema.type.field
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::List(list) = node {
                        let parts = extract_column_parts_from_list(&list.items)?;
                        let parent_name = QualifiedIdent::new(parts.0, parts.1);
                        
                        // For columns, we need to add dependency on both tables and types
                        // since we can't tell from parsing alone whether it's a table or type
                        dependencies.relations.insert(parent_name.clone());
                        dependencies.types.insert(parent_name.clone());
                        
                        let comment_id = QualifiedIdent::new(
                            None,
                            format!("column:{}.{}", format_qualified_name(&parent_name), parts.2)
                        );
                        return Ok((comment_id, dependencies));
                    }
                }
            }
        }
        PgObjectType::ObjectFunction => {
            // COMMENT ON FUNCTION schema.func_name(args)
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::ObjectWithArgs(func_with_args) = node {
                        let qualified_name = extract_name_from_node_list(&func_with_args.objname)?;
                        dependencies.functions.insert(qualified_name.clone());
                        
                        // Generate function signature for unique identification
                        let signature = format_function_signature(&qualified_name, &func_with_args.objargs);
                        let comment_id = QualifiedIdent::new(
                            None,
                            format!("function:{}", signature)
                        );
                        return Ok((comment_id, dependencies));
                    }
                }
            }
        }
        PgObjectType::ObjectType => {
            // COMMENT ON TYPE schema.type_name
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::TypeName(type_name) = node {
                        let qualified_name = extract_name_from_node_list(&type_name.names)?;
                        dependencies.types.insert(qualified_name.clone());
                        let comment_id = QualifiedIdent::new(
                            None,
                            format!("type:{}", format_qualified_name(&qualified_name))
                        );
                        return Ok((comment_id, dependencies));
                    }
                }
            }
        }
        PgObjectType::ObjectTrigger => {
            // COMMENT ON TRIGGER trigger_name ON table_name
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::List(list) = node {
                        // For triggers, parse as [table_parts..., trigger_name]
                        if list.items.len() >= 2 {
                            // The trigger name is always the last element
                            let trigger_name = extract_string_from_node(&list.items[list.items.len() - 1])?;
                            
                            // Table name elements are everything except the last
                            let table_parts: Result<Vec<String>, _> = list.items[..list.items.len() - 1]
                                .iter()
                                .map(extract_string_from_node)
                                .collect();
                            let table_parts = table_parts?;
                            
                            // Parse table name (could be qualified)
                            let table_qualified = match table_parts.len() {
                                1 => QualifiedIdent::from_name(table_parts[0].clone()),
                                2 => QualifiedIdent::new(Some(table_parts[0].clone()), table_parts[1].clone()),
                                _ => return Err("Invalid table name in trigger comment".into()),
                            };
                            
                            dependencies.relations.insert(table_qualified.clone());
                            let comment_id = QualifiedIdent::new(
                                None, // Triggers are not schema-qualified in comments
                                format!("trigger:{}:{}", trigger_name, format_qualified_name(&table_qualified))
                            );
                            
                            return Ok((comment_id, dependencies));
                        }
                    }
                }
            }
        }
        PgObjectType::ObjectDomain => {
            // COMMENT ON DOMAIN schema.domain_name
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::TypeName(type_name) = node {
                        let qualified_name = extract_name_from_node_list(&type_name.names)?;
                        dependencies.types.insert(qualified_name.clone()); // Domains are a type of type
                        let comment_id = QualifiedIdent::new(
                            None,
                            format!("domain:{}", format_qualified_name(&qualified_name))
                        );
                        return Ok((comment_id, dependencies));
                    }
                }
            }
        }
        PgObjectType::ObjectView => {
            // COMMENT ON VIEW schema.view_name
            if let Some(object) = &comment_stmt.object {
                if let Some(node) = &object.node {
                    if let pg_query::NodeEnum::List(list) = node {
                        let qualified_name = extract_name_from_node_list(&list.items)?;
                        // View comments depend on the view
                        dependencies.relations.insert(qualified_name.clone());
                        let comment_id = QualifiedIdent::new(
                            None,
                            format!("view:{}", format_qualified_name(&qualified_name))
                        );
                        return Ok((comment_id, dependencies));
                    }
                }
            }
        }
        _ => {
            return Err(format!("Unsupported comment target type: {:?}", comment_stmt.objtype()).into());
        }
    }
    
    Err("Could not parse comment target".into())
}

/// Helper to extract column parts (schema, table, column) from a node list
fn extract_column_parts_from_list(items: &[pg_query::protobuf::Node]) -> Result<(Option<String>, String, String), Box<dyn std::error::Error>> {
    if items.len() < 2 {
        return Err("Invalid column reference".into());
    }
    
    // Extract table and column names
    let table_name = extract_string_from_node(&items[0])?;
    let column_name = extract_string_from_node(&items[1])?;
    
    // Check if there's a schema
    let (schema, table) = if items.len() == 3 {
        let schema = extract_string_from_node(&items[0])?;
        let table = extract_string_from_node(&items[1])?;
        let column = extract_string_from_node(&items[2])?;
        return Ok((Some(schema), table, column));
    } else {
        (None, table_name)
    };
    
    Ok((schema, table, column_name))
}

/// Helper to format function signature for unique identification
fn format_function_signature(qualified_name: &QualifiedIdent, _args: &[pg_query::protobuf::Node]) -> String {
    // For now, we'll use a simplified approach without parsing full argument types
    // In practice, PostgreSQL handles function overloading, but we've blocked that in pgmg
    format!("{}()", format_qualified_name(qualified_name))
}

/// Helper to format a qualified name
fn format_qualified_name(qualified_name: &QualifiedIdent) -> String {
    match &qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, qualified_name.name),
        None => qualified_name.name.clone(),
    }
}

/// Helper to extract string value from a node
fn extract_string_from_node(node: &pg_query::protobuf::Node) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(node_type) = &node.node {
        if let pg_query::NodeEnum::String(string_node) = node_type {
            return Ok(string_node.sval.clone());
        }
    }
    Err("Node is not a string".into())
}

/// Helper to extract qualified name from a list of nodes
fn extract_name_from_node_list(items: &[pg_query::protobuf::Node]) -> Result<QualifiedIdent, Box<dyn std::error::Error>> {
    match items.len() {
        1 => {
            let name = extract_string_from_node(&items[0])?;
            Ok(QualifiedIdent::from_name(name))
        }
        2 => {
            let schema = extract_string_from_node(&items[0])?;
            let name = extract_string_from_node(&items[1])?;
            Ok(QualifiedIdent::new(Some(schema), name))
        }
        _ => Err("Invalid qualified name".into()),
    }
}

/// Parse a statement to check if it's a cron.schedule() or cron.unschedule() call
fn parse_cron_call_from_statement(statement: &str) -> Result<Option<CronOperation>, Box<dyn std::error::Error>> {
    // Parse the statement to get the AST
    let parsed = pg_query::parse(statement)?;
    
    // Look for SELECT statements or direct function calls
    for stmt in &parsed.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            match &stmt.node {
                Some(pg_query::NodeEnum::SelectStmt(select)) => {
                    // Check target list for function calls
                    for target in &select.target_list {
                        if let Some(pg_query::NodeEnum::ResTarget(res_target)) = &target.node {
                            if let Some(val) = &res_target.val {
                                if let Some(pg_query::NodeEnum::FuncCall(func_call)) = &val.node {
                                    // Check if this is cron.schedule or cron.unschedule
                                    if let Some(op) = parse_cron_function_call(func_call)? {
                                        return Ok(Some(op));
                                    }
                                }
                            }
                        }
                    }
                }
                Some(pg_query::NodeEnum::CallStmt(call)) => {
                    // Handle CALL statements (though cron typically uses SELECT)
                    if let Some(func_call) = &call.funccall {
                        if let Some(op) = parse_cron_function_call(func_call)? {
                            return Ok(Some(op));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    
    Ok(None)
}

/// Parse a cron function call and return the operation type
fn parse_cron_function_call(func_call: &pg_query::protobuf::FuncCall) -> Result<Option<CronOperation>, Box<dyn std::error::Error>> {
    // Extract function name parts
    let parts: Vec<String> = func_call.funcname.iter()
        .filter_map(|node| {
            if let Some(pg_query::NodeEnum::String(s)) = &node.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    // Check if it's cron.schedule or cron.unschedule
    let is_schedule = match parts.as_slice() {
        [schema, func] if schema == "cron" => {
            match func.as_str() {
                "schedule" => Some(true),
                "unschedule" => Some(false),
                _ => None,
            }
        }
        [func] => {
            // Might be in search path
            match func.as_str() {
                "schedule" => Some(true),
                "unschedule" => Some(false),
                _ => None,
            }
        }
        _ => None,
    };
    
    match is_schedule {
        Some(true) => {
            // Extract job name and command from arguments
            if let Some((job_name, command)) = extract_cron_schedule_args(func_call)? {
                Ok(Some(CronOperation::Schedule { job_name, command }))
            } else {
                Ok(None)
            }
        }
        Some(false) => {
            // Extract job name from first argument
            if let Some(job_name) = extract_cron_job_name(func_call)? {
                Ok(Some(CronOperation::Unschedule { job_name }))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

/// Extract arguments from cron.schedule function call
fn extract_cron_schedule_args(func_call: &pg_query::protobuf::FuncCall) -> Result<Option<(String, String)>, Box<dyn std::error::Error>> {
    // cron.schedule expects 3 arguments: job_name, schedule, command
    if func_call.args.len() < 3 {
        return Ok(None);
    }
    
    // Extract job name (1st argument)
    let job_name = match extract_string_from_const_node(&func_call.args[0]) {
        Some(name) => name,
        None => return Ok(None),
    };
    
    // Skip schedule (2nd argument) - we don't need it for dependencies
    
    // Extract command (3rd argument)
    let command = match extract_string_from_const_node(&func_call.args[2]) {
        Some(cmd) => cmd,
        None => return Ok(None),
    };
    
    Ok(Some((job_name, command)))
}

/// Extract job name from cron.unschedule function call (first argument)
fn extract_cron_job_name(func_call: &pg_query::protobuf::FuncCall) -> Result<Option<String>, Box<dyn std::error::Error>> {
    // The first argument should be the job name (a string constant)
    if let Some(first_arg) = func_call.args.first() {
        // Try to extract the string value from the argument
        if let Some(job_name) = extract_string_from_const_node(first_arg) {
            return Ok(Some(job_name));
        }
    }
    Ok(None)
}

/// Helper to extract string value from a constant node (handles AConst wrapping)
fn extract_string_from_const_node(node: &pg_query::protobuf::Node) -> Option<String> {
    match &node.node {
        Some(pg_query::NodeEnum::AConst(a_const)) => {
            // For AConst, we need to check if it contains a String
            // Based on protobuf structure, val should be an oneof field
            match &a_const.val {
                Some(pg_query::protobuf::a_const::Val::Sval(string_val)) => {
                    Some(string_val.sval.clone())
                }
                _ => None,
            }
        }
        Some(pg_query::NodeEnum::String(string_val)) => {
            // Direct string node
            Some(string_val.sval.clone())
        }
        _ => None,
    }
}

/// Parse dependencies from a cron command string (e.g., "CALL jobs.update_stats()")
fn parse_cron_command_dependencies(command: &str) -> Dependencies {
    // Try to parse the command as SQL
    match crate::sql::parser::analyze_statement(command) {
        Ok(deps) => deps,
        Err(e) => {
            // If parsing fails, log a warning and return empty dependencies
            eprintln!("Warning: Failed to parse cron command '{}': {}", command, e);
            Dependencies {
                relations: std::collections::HashSet::new(),
                functions: std::collections::HashSet::new(),
                types: std::collections::HashSet::new(),
            }
        }
    }
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
    fn test_extract_function_signature() {
        // Test function without parameters
        let sql = "CREATE FUNCTION test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql";
        let signature = extract_function_signature(sql).unwrap();
        assert_eq!(signature, "test_func()");
        
        // Test function with parameters
        let sql = r#"CREATE FUNCTION api.create_stripe_event(
            p_event_id   text,
            p_event_type text,
            p_payload    jsonb,
            p_object_id  text default null
        ) RETURNS void LANGUAGE sql AS $$ SELECT 1; $$"#;
        let signature = extract_function_signature(sql).unwrap();
        assert_eq!(signature, "api.create_stripe_event(text, text, jsonb, text)");
        
        // Test function with schema and multiple parameters
        let sql = "CREATE FUNCTION myschema.calculate(a integer, b numeric) RETURNS numeric LANGUAGE sql AS $$ SELECT $1 + $2; $$";
        let signature = extract_function_signature(sql).unwrap();
        assert_eq!(signature, "myschema.calculate(pg_catalog.int4, pg_catalog.numeric)");
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
    fn test_identify_create_procedure() {
        let sql = r#"
        CREATE PROCEDURE process_data(batch_size INT) 
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM process_batch(batch_size);
            COMMIT;
        END;
        $$;
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Procedure);
        assert_eq!(obj.qualified_name.name, "process_data");
    }

    #[test]
    fn test_identify_qualified_procedure() {
        let sql = r#"
        CREATE OR REPLACE PROCEDURE api.update_stats() 
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE stats SET last_run = NOW();
        END;
        $$;
        "#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Procedure);
        assert_eq!(obj.qualified_name.name, "update_stats");
        assert_eq!(obj.qualified_name.schema, Some("api".to_string()));
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
    fn test_identify_cron_schedule() {
        let sql = "SELECT cron.schedule('cleanup_old_data', '0 3 * * *', 'CALL cleanup_old_data()');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "cleanup_old_data");
        assert!(obj.qualified_name.schema.is_none());
    }
    
    #[test]
    fn test_identify_cron_schedule_with_schema() {
        let sql = "SELECT cron.schedule('vacuum_job', '30 2 * * *', 'VACUUM ANALYZE api.events;');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "vacuum_job");
        // Note: The job name doesn't have a schema, even if the command references a schema
        assert!(obj.qualified_name.schema.is_none());
    }
    
    #[test]
    fn test_identify_cron_unschedule() {
        // cron.unschedule should not create a parsed object
        let sql = "SELECT cron.unschedule('cleanup_old_data');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_none()); // Unschedule doesn't create an object
    }
    
    #[test]
    fn test_cron_schedule_dependencies() {
        let sql = "SELECT cron.schedule('update_stats', '0 * * * *', 'CALL jobs.update_user_stats()');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "update_stats");
        
        // Should extract dependency on the procedure being called
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::new(
            Some("jobs".to_string()),
            "update_user_stats".to_string()
        )));
        
        // Should NOT include cron.schedule as a dependency
        assert!(!obj.dependencies.functions.contains(&QualifiedIdent::new(
            Some("cron".to_string()),
            "schedule".to_string()
        )));
    }
    
    #[test]
    fn test_cron_schedule_vacuum_dependencies() {
        let sql = "SELECT cron.schedule('vacuum_events', '0 2 * * *', 'VACUUM ANALYZE api.events');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "vacuum_events");
        
        // Should extract dependency on the table being vacuumed
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::new(
            Some("api".to_string()),
            "events".to_string()
        )));
    }
    
    #[test]
    fn test_cron_schedule_select_function_dependencies() {
        let sql = "SELECT cron.schedule('refresh_cache', '*/15 * * * *', 'SELECT public.refresh_materialized_views()');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "refresh_cache");
        
        // Should extract dependency on the function being called
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::new(
            Some("public".to_string()),
            "refresh_materialized_views".to_string()
        )));
    }
    
    #[test]
    fn test_cron_schedule_delete_dependencies() {
        let sql = "SELECT cron.schedule('cleanup_logs', '0 1 * * *', 'DELETE FROM logs WHERE created_at < now() - interval ''30 days''');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "cleanup_logs");
        
        // Should extract dependency on the table being deleted from
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::from_name("logs".to_string())));
    }
    
    #[test]
    fn test_cron_schedule_update_dependencies() {
        let sql = "SELECT cron.schedule('update_counts', '0 * * * *', 'UPDATE stats SET last_updated = now()');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "update_counts");
        
        // Should extract dependency on the table being updated
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::from_name("stats".to_string())));
    }
    
    #[test]
    fn test_cron_schedule_invalid_command() {
        // Test with invalid SQL in command - should still create the cron job
        let sql = "SELECT cron.schedule('bad_job', '0 * * * *', 'NOT VALID SQL SYNTAX');";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::CronJob);
        assert_eq!(obj.qualified_name.name, "bad_job");
        
        // Should have no dependencies (parsing failed)
        assert!(obj.dependencies.relations.is_empty());
        assert!(obj.dependencies.functions.is_empty());
        assert!(obj.dependencies.types.is_empty());
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
    
    #[test]
    fn test_comment_on_table() {
        let sql = "COMMENT ON TABLE users IS 'User account information'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "table:users");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the table
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::from_name("users".to_string())));
    }
    
    #[test]
    fn test_comment_on_qualified_table() {
        let sql = "COMMENT ON TABLE api.users IS 'API user table'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "table:api.users");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the qualified table
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
    }
    
    #[test]
    fn test_comment_on_column() {
        let sql = "COMMENT ON COLUMN users.email IS 'User email address'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "column:users.email");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the table (and potentially type)
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::from_name("users".to_string())));
    }
    
    #[test]
    fn test_comment_on_qualified_column() {
        let sql = "COMMENT ON COLUMN api.users.email IS 'User email in API schema'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "column:api.users.email");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the qualified table
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
    }
    
    #[test]
    fn test_comment_on_type_field() {
        let sql = "COMMENT ON COLUMN address.street IS 'Street address field'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "column:address.street");
        
        // Should have dependency on both relations and types (since we can't distinguish)
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::from_name("address".to_string())));
        assert!(obj.dependencies.types.contains(&QualifiedIdent::from_name("address".to_string())));
    }
    
    #[test]
    fn test_comment_on_function() {
        let sql = "COMMENT ON FUNCTION get_user_stats() IS 'Calculates user statistics'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "function:get_user_stats()");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the function
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::from_name("get_user_stats".to_string())));
    }
    
    #[test]
    fn test_comment_on_qualified_function() {
        let sql = "COMMENT ON FUNCTION api.get_user_stats() IS 'API function for user stats'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "function:api.get_user_stats()");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the qualified function
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::new(Some("api".to_string()), "get_user_stats".to_string())));
    }
    
    #[test]
    fn test_comment_on_type() {
        let sql = "COMMENT ON TYPE address IS 'Address composite type'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "type:address");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the type
        assert!(obj.dependencies.types.contains(&QualifiedIdent::from_name("address".to_string())));
    }
    
    #[test]
    fn test_comment_on_domain() {
        let sql = "COMMENT ON DOMAIN email_address IS 'Valid email address domain'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "domain:email_address");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the domain (as a type)
        assert!(obj.dependencies.types.contains(&QualifiedIdent::from_name("email_address".to_string())));
    }
    
    #[test]
    fn test_comment_on_trigger() {
        let sql = "COMMENT ON TRIGGER update_timestamp ON users IS 'Updates timestamp on row changes'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "trigger:update_timestamp:users");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the table
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::from_name("users".to_string())));
    }
    
    #[test]
    fn test_comment_on_qualified_trigger() {
        let sql = "COMMENT ON TRIGGER update_stats ON api.users IS 'API trigger for stats'";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "trigger:update_stats:api.users");
        assert!(obj.qualified_name.schema.is_none()); // Triggers themselves are not schema-qualified in comments
        
        // Should have dependency on the qualified table
        assert!(obj.dependencies.relations.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
    }
    
    #[test]
    fn test_comment_with_multiline_text() {
        let sql = r#"COMMENT ON FUNCTION calculate_total(DECIMAL, DECIMAL) IS $$
        Calculates the total amount including tax.
        
        Parameters:
        - base: Base amount
        - tax: Tax amount
        
        Returns: Total amount
        $$"#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Comment);
        assert_eq!(obj.qualified_name.name, "function:calculate_total()");
        
        // Should have dependency on the function
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::from_name("calculate_total".to_string())));
    }
    
    #[test]
    fn test_non_comment_statement() {
        let sql = "SELECT * FROM users";
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_none()); // Not a DDL statement we track
    }
    
    #[test]
    fn test_identify_create_aggregate() {
        let sql = r#"CREATE AGGREGATE sum(currency) (
            sfunc = currency_sum_sfunc,
            stype = currency,
            parallel = safe
        );"#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Aggregate);
        assert_eq!(obj.qualified_name.name, "sum");
        assert!(obj.qualified_name.schema.is_none());
        
        // Should have dependency on the state function
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::from_name("currency_sum_sfunc".to_string())));
    }
    
    #[test]
    fn test_identify_qualified_aggregate() {
        let sql = r#"CREATE AGGREGATE public.array_agg_comp(anyelement) (
            sfunc = array_comp_sfunc,
            stype = anyarray,
            initcond = '{}'
        );"#;
        let result = identify_sql_object(sql).unwrap();
        
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.object_type, ObjectType::Aggregate);
        assert_eq!(obj.qualified_name.name, "array_agg_comp");
        assert_eq!(obj.qualified_name.schema, Some("public".to_string()));
        
        // Should have dependency on the state function
        assert!(obj.dependencies.functions.contains(&QualifiedIdent::from_name("array_comp_sfunc".to_string())));
    }
}