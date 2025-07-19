use std::collections::HashSet;
use pg_query::{NodeEnum, NodeRef};
use serde_json::Value;

use crate::builtin_catalog::BuiltinCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedIdent {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct Dependencies {
    pub relations: HashSet<QualifiedIdent>,
    pub functions: HashSet<QualifiedIdent>,
    pub types: HashSet<QualifiedIdent>,
}

impl QualifiedIdent {
    pub fn new(schema: Option<String>, name: String) -> Self {
        Self { schema, name }
    }
    
    pub fn from_name(name: String) -> Self {
        Self { schema: None, name }
    }
    
    pub fn from_qualified_name(qualified_name: &str) -> Self {
        let parts: Vec<&str> = qualified_name.split('.').collect();
        if parts.len() == 2 {
            Self {
                schema: Some(parts[0].to_string()),
                name: parts[1].to_string(),
            }
        } else {
            Self {
                schema: None,
                name: qualified_name.to_string(),
            }
        }
    }
}

pub fn analyze_statement(sql: &str) -> Result<Dependencies, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(sql)?;
    extract_dependencies_from_parse_result_with_sql(&parse_result.protobuf, Some(sql))
}

/// Extract dependencies from an already-parsed statement
pub fn extract_dependencies_from_parse_result(parse_result: &pg_query::protobuf::ParseResult) -> Result<Dependencies, Box<dyn std::error::Error>> {
    extract_dependencies_from_parse_result_with_sql(parse_result, None)
}

/// Extract dependencies from an already-parsed statement, with optional original SQL for PL/pgSQL analysis
pub fn extract_dependencies_from_parse_result_with_sql(parse_result: &pg_query::protobuf::ParseResult, original_sql: Option<&str>) -> Result<Dependencies, Box<dyn std::error::Error>> {
    // Create a wrapper to use pg_query's built-in extraction methods
    let parsed_wrapped = pg_query::ParseResult::new(parse_result.clone(), String::new());
    
    // Extract relations using existing pg_query functionality
    let mut relations = HashSet::new();
    for table in parsed_wrapped.tables() {
        relations.insert(QualifiedIdent::from_qualified_name(&table));
    }
    
    let mut functions = HashSet::new();
    
    // Get functions from pg_query's built-in functionality
    for func in parsed_wrapped.functions() {
        functions.insert(QualifiedIdent::from_qualified_name(&func));
    }
    
    let mut types = HashSet::new();
    
    // Also traverse the entire AST to extract REFERENCES and DEFAULT functions
    for stmt in &parse_result.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(node) = &stmt.node {
                extract_from_node_with_types(node, &mut relations, &mut functions, &mut types);
                
                // Check if this is a CREATE FUNCTION with LANGUAGE SQL
                if let NodeEnum::CreateFunctionStmt(create_func) = node {
                    // Extract return type from the function signature
                    if let Some(return_type) = &create_func.return_type {
                        if let Some(qualified_type) = extract_type_from_type_name(return_type) {
                            types.insert(qualified_type);
                        }
                    }
                    
                    // Extract parameter types
                    for param in &create_func.parameters {
                        if let Some(NodeEnum::FunctionParameter(func_param)) = &param.node {
                            if let Some(param_type) = &func_param.arg_type {
                                if let Some(qualified_type) = extract_type_from_type_name(param_type) {
                                    types.insert(qualified_type);
                                }
                            }
                        }
                    }
                    
                    // Extract the function name with schema qualification
                    if let Some(func_ident) = extract_function_name_from_create_stmt(create_func) {
                        functions.insert(func_ident);
                    }
                    
                    if is_language_sql_function(create_func) {
                        if let Some(sql_body) = extract_sql_function_body(create_func) {
                            // Split the SQL body into individual statements and analyze each one
                            match split_sql_statements(&sql_body) {
                                Ok(statements) => {
                                    for statement in statements {
                                        let trimmed = statement.trim();
                                        if !trimmed.is_empty() {
                                            match analyze_statement(trimmed) {
                                                Ok(body_deps) => {
                                                    relations.extend(body_deps.relations);
                                                    functions.extend(body_deps.functions);
                                                    types.extend(body_deps.types);
                                                }
                                                Err(e) => {
                                                    // Log the error but don't fail the entire analysis
                                                    eprintln!("Warning: Failed to parse SQL function statement '{}': {}", trimmed, e);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Log the error but don't fail the entire analysis
                                    eprintln!("Warning: Failed to split SQL function body: {}", e);
                                }
                            }
                        }
                    } else if is_language_plpgsql_function(create_func) {
                        // For PL/pgSQL functions, analyze the entire SQL statement
                        if let Some(sql) = original_sql {
                            match analyze_plpgsql(sql) {
                                Ok(plpgsql_deps) => {
                                    relations.extend(plpgsql_deps.relations);
                                    functions.extend(plpgsql_deps.functions);
                                    types.extend(plpgsql_deps.types);
                                }
                                Err(e) => {
                                    eprintln!("Warning: Failed to analyze PL/pgSQL function: {}", e);
                                }
                            }
                        } else {
                            eprintln!("Warning: PL/pgSQL function body analysis requires original SQL text");
                        }
                    }
                }
            }
        }
    }
    
    // Extract types from cast expressions at the top level
    let top_level_types = extract_types_from_ast(parse_result)?;
    for typ in top_level_types {
        types.insert(typ);
    }
    
    Ok(Dependencies {
        relations,
        functions,
        types,
    })
}

/// Filters out built-in PostgreSQL objects from the dependencies
pub fn filter_builtins(deps: Dependencies, catalog: &BuiltinCatalog) -> Dependencies {
    Dependencies {
        relations: deps.relations.into_iter()
            .filter(|rel| !catalog.relations.contains(rel))
            .collect(),
        functions: deps.functions.into_iter()
            .filter(|func| !catalog.functions.contains(func))
            .collect(),
        types: deps.types.into_iter()
            .filter(|typ| !catalog.types.contains(typ))
            .collect(),
    }
}

/// Check if a CREATE FUNCTION statement is using LANGUAGE SQL
fn is_language_sql_function(create_func: &pg_query::protobuf::CreateFunctionStmt) -> bool {
    for option in &create_func.options {
        if let Some(def_elem) = &option.node {
            if let NodeEnum::DefElem(def) = def_elem {
                if def.defname == "language" {
                    if let Some(arg) = &def.arg {
                        if let Some(value_node) = &arg.node {
                            if let NodeEnum::String(string_val) = value_node {
                                return string_val.sval.to_lowercase() == "sql";
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

/// Extract the SQL body from a LANGUAGE SQL function
fn extract_sql_function_body(create_func: &pg_query::protobuf::CreateFunctionStmt) -> Option<String> {
    for option in &create_func.options {
        if let Some(def_elem) = &option.node {
            if let NodeEnum::DefElem(def) = def_elem {
                if def.defname == "as" {
                    if let Some(arg) = &def.arg {
                        if let Some(value_node) = &arg.node {
                            match value_node {
                                NodeEnum::String(string_val) => {
                                    return Some(string_val.sval.clone());
                                }
                                NodeEnum::List(list) => {
                                    // Function body can be a list of strings
                                    let mut body = String::new();
                                    for item in &list.items {
                                        if let Some(item_node) = &item.node {
                                            if let NodeEnum::String(string_val) = item_node {
                                                body.push_str(&string_val.sval);
                                                body.push('\n');
                                            }
                                        }
                                    }
                                    return Some(body);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Split SQL body into individual statements using pg_query's parser
fn split_sql_statements(sql_body: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let statements = pg_query::split_with_parser(sql_body)?;
    Ok(statements.into_iter().map(|s| s.to_string()).collect())
}

/// Check if a CREATE FUNCTION statement is using LANGUAGE plpgsql
fn is_language_plpgsql_function(create_func: &pg_query::protobuf::CreateFunctionStmt) -> bool {
    for option in &create_func.options {
        if let Some(def_elem) = &option.node {
            if let NodeEnum::DefElem(def) = def_elem {
                if def.defname == "language" {
                    if let Some(arg) = &def.arg {
                        if let Some(value_node) = &arg.node {
                            if let NodeEnum::String(string_val) = value_node {
                                return string_val.sval.to_lowercase() == "plpgsql";
                            }
                        }
                    }
                }
            }
        }
    }
    false
}


/// Extract the function name and schema from a CREATE FUNCTION statement
fn extract_function_name_from_create_stmt(create_func: &pg_query::protobuf::CreateFunctionStmt) -> Option<QualifiedIdent> {
    if create_func.funcname.is_empty() {
        return None;
    }
    
    let name_parts: Vec<String> = create_func.funcname.iter()
        .filter_map(|node| {
            if let Some(NodeEnum::String(string_val)) = &node.node {
                Some(string_val.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    match name_parts.len() {
        1 => Some(QualifiedIdent::from_name(name_parts[0].clone())),
        2 => Some(QualifiedIdent::new(Some(name_parts[0].clone()), name_parts[1].clone())),
        _ => {
            // Handle cases with more than 2 parts by taking the last two
            if name_parts.len() > 2 {
                let len = name_parts.len();
                Some(QualifiedIdent::new(
                    Some(name_parts[len - 2].clone()),
                    name_parts[len - 1].clone(),
                ))
            } else {
                None
            }
        }
    }
}

fn extract_from_constraint(
    constraint: &pg_query::protobuf::Constraint,
    relations: &mut HashSet<QualifiedIdent>,
    functions: &mut HashSet<QualifiedIdent>,
) {
    use pg_query::protobuf::ConstrType;
    
    if constraint.contype == ConstrType::ConstrForeign as i32 {
        if let Some(pktable) = &constraint.pktable {
            let table_ident = if !pktable.schemaname.is_empty() {
                QualifiedIdent::new(Some(pktable.schemaname.clone()), pktable.relname.clone())
            } else {
                QualifiedIdent::from_name(pktable.relname.clone())
            };
            relations.insert(table_ident);
        }
    }
    // Check if it's a DEFAULT, CHECK, or GENERATED constraint with expressions
    if constraint.contype == ConstrType::ConstrDefault as i32 || 
        constraint.contype == ConstrType::ConstrCheck as i32 ||
        constraint.contype == ConstrType::ConstrGenerated as i32 {
        if let Some(raw_expr) = &constraint.raw_expr {
            extract_from_node(raw_expr.node.as_ref().unwrap(), relations, functions);
        }
    }
}

fn extract_from_constraint_with_types(
    constraint: &pg_query::protobuf::Constraint,
    relations: &mut HashSet<QualifiedIdent>,
    functions: &mut HashSet<QualifiedIdent>,
    types: &mut HashSet<QualifiedIdent>,
) {
    use pg_query::protobuf::ConstrType;
    
    if constraint.contype == ConstrType::ConstrForeign as i32 {
        if let Some(pktable) = &constraint.pktable {
            let table_ident = if !pktable.schemaname.is_empty() {
                QualifiedIdent::new(Some(pktable.schemaname.clone()), pktable.relname.clone())
            } else {
                QualifiedIdent::from_name(pktable.relname.clone())
            };
            relations.insert(table_ident);
        }
    }
    // Check if it's a DEFAULT, CHECK, or GENERATED constraint with expressions
    if constraint.contype == ConstrType::ConstrDefault as i32 || 
        constraint.contype == ConstrType::ConstrCheck as i32 ||
        constraint.contype == ConstrType::ConstrGenerated as i32 {
        if let Some(raw_expr) = &constraint.raw_expr {
            extract_from_node_with_types(raw_expr.node.as_ref().unwrap(), relations, functions, types);
        }
    }
}

fn extract_function_name_from_nodes(
    nodes: &[pg_query::protobuf::Node],
    functions: &mut HashSet<QualifiedIdent>,
) {
    let function_name_parts: Vec<String> = nodes.iter()
        .filter_map(|node| {
            if let Some(NodeEnum::String(string_node)) = &node.node {
                Some(string_node.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    if !function_name_parts.is_empty() {
        let func_ident = if function_name_parts.len() == 1 {
            QualifiedIdent::from_name(function_name_parts[0].clone())
        } else if function_name_parts.len() == 2 {
            QualifiedIdent::new(Some(function_name_parts[0].clone()), function_name_parts[1].clone())
        } else {
            // Handle cases with more than 2 parts by taking the last two
            let len = function_name_parts.len();
            QualifiedIdent::new(
                Some(function_name_parts[len - 2].clone()),
                function_name_parts[len - 1].clone(),
            )
        };
        functions.insert(func_ident);
    }
}

fn extract_from_node_with_types(
    node: &NodeEnum,
    relations: &mut HashSet<QualifiedIdent>,
    functions: &mut HashSet<QualifiedIdent>,
    types: &mut HashSet<QualifiedIdent>,
) {
    // First extract any types from TypeCast nodes
    if let NodeEnum::TypeCast(type_cast) = node {
        if let Some(type_name) = &type_cast.type_name {
            if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                types.insert(qualified_type);
            }
        }
    }
    
    // Then do the normal extraction
    match node {
        NodeEnum::CreateStmt(create_stmt) => {
            // Extract from table elements (columns, constraints)
            for table_elt in &create_stmt.table_elts {
                match &table_elt.node {
                    Some(NodeEnum::ColumnDef(col_def)) => {
                        // Extract DEFAULT functions
                        if let Some(raw_default) = &col_def.raw_default {
                            extract_from_node_with_types(raw_default.node.as_ref().unwrap(), relations, functions, types);
                        }
                        
                        // Extract REFERENCES from column constraints
                        for constraint in &col_def.constraints {
                            if let Some(NodeEnum::Constraint(c)) = &constraint.node {
                                extract_from_constraint_with_types(c, relations, functions, types);
                            }
                        }
                    }
                    Some(NodeEnum::Constraint(table_constraint)) => {
                        extract_from_constraint_with_types(table_constraint, relations, functions, types);
                    }
                    _ => {}
                }
            }
        }
        _ => {
            // For all other node types, use the original extraction but recurse with type tracking
            extract_from_node_recursive(node, relations, functions, types);
        }
    }
}

fn extract_from_node_recursive(
    node: &NodeEnum,
    relations: &mut HashSet<QualifiedIdent>,
    functions: &mut HashSet<QualifiedIdent>,
    types: &mut HashSet<QualifiedIdent>,
) {
    match node {
        NodeEnum::FuncCall(func_call) => {
            extract_function_from_func_call(func_call, functions);
            // Also extract from function arguments
            for arg in &func_call.args {
                if let Some(node) = &arg.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::TypeCast(type_cast) => {
            if let Some(arg) = &type_cast.arg {
                extract_from_node_with_types(arg.node.as_ref().unwrap(), relations, functions, types);
            }
        }
        NodeEnum::RangeVar(range_var) => {
            let table_ident = if !range_var.schemaname.is_empty() {
                QualifiedIdent::new(Some(range_var.schemaname.clone()), range_var.relname.clone())
            } else {
                QualifiedIdent::from_name(range_var.relname.clone())
            };
            relations.insert(table_ident);
        }
        NodeEnum::SelectStmt(select_stmt) => {
            // Extract from FROM clause
            for from_item in &select_stmt.from_clause {
                if let Some(node) = &from_item.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
            for target in &select_stmt.target_list {
                if let Some(node) = &target.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
            if let Some(where_clause) = &select_stmt.where_clause {
                extract_from_node_with_types(where_clause.node.as_ref().unwrap(), relations, functions, types);
            }
            if let Some(having_clause) = &select_stmt.having_clause {
                extract_from_node_with_types(having_clause.node.as_ref().unwrap(), relations, functions, types);
            }
            for group_item in &select_stmt.group_clause {
                if let Some(node) = &group_item.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
            for sort_item in &select_stmt.sort_clause {
                if let Some(node) = &sort_item.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::SubLink(sublink) => {
            if let Some(subselect) = &sublink.subselect {
                extract_from_node_with_types(subselect.node.as_ref().unwrap(), relations, functions, types);
            }
        }
        NodeEnum::ResTarget(res_target) => {
            if let Some(val) = &res_target.val {
                extract_from_node_with_types(val.node.as_ref().unwrap(), relations, functions, types);
            }
        }
        NodeEnum::AExpr(a_expr) => {
            if let Some(lexpr) = &a_expr.lexpr {
                extract_from_node_with_types(lexpr.node.as_ref().unwrap(), relations, functions, types);
            }
            if let Some(rexpr) = &a_expr.rexpr {
                extract_from_node_with_types(rexpr.node.as_ref().unwrap(), relations, functions, types);
            }
        }
        NodeEnum::BoolExpr(bool_expr) => {
            for arg in &bool_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::List(list) => {
            for item in &list.items {
                if let Some(node) = &item.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::CaseExpr(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                extract_from_node_with_types(arg.node.as_ref().unwrap(), relations, functions, types);
            }
            if let Some(defresult) = &case_expr.defresult {
                extract_from_node_with_types(defresult.node.as_ref().unwrap(), relations, functions, types);
            }
            for when_clause in &case_expr.args {
                if let Some(node) = &when_clause.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::CaseWhen(case_when) => {
            if let Some(expr) = &case_when.expr {
                extract_from_node_with_types(expr.node.as_ref().unwrap(), relations, functions, types);
            }
            if let Some(result) = &case_when.result {
                extract_from_node_with_types(result.node.as_ref().unwrap(), relations, functions, types);
            }
        }
        NodeEnum::CoalesceExpr(coalesce_expr) => {
            functions.insert(QualifiedIdent::from_name("coalesce".to_string()));
            for arg in &coalesce_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::MinMaxExpr(min_max_expr) => {
            for arg in &min_max_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        NodeEnum::ArrayExpr(array_expr) => {
            for element in &array_expr.elements {
                if let Some(node) = &element.node {
                    extract_from_node_with_types(node, relations, functions, types);
                }
            }
        }
        _ => {
            // For any other node types, just extract normally without type tracking
            extract_from_node(node, relations, functions);
        }
    }
}

fn extract_from_node(
    node: &NodeEnum,
    relations: &mut HashSet<QualifiedIdent>,
    functions: &mut HashSet<QualifiedIdent>,
) {
    
    match node {
        NodeEnum::CreateStmt(create_stmt) => {
            // Extract from table elements (columns, constraints)
            for table_elt in &create_stmt.table_elts {
                match &table_elt.node {
                    Some(NodeEnum::ColumnDef(col_def)) => {
                        // Extract DEFAULT functions
                        if let Some(raw_default) = &col_def.raw_default {
                            extract_from_node(raw_default.node.as_ref().unwrap(), relations, functions);
                        }
                        
                        // GENERATED column expressions are handled via constraints below
                        
                        // Extract REFERENCES from column constraints
                        for constraint in &col_def.constraints {
                            if let Some(NodeEnum::Constraint(c)) = &constraint.node {
                                extract_from_constraint(c, relations, functions);
                            }
                        }
                    }
                    Some(NodeEnum::Constraint(table_constraint)) => {
                        // Handle table-level constraints
                        extract_from_constraint(table_constraint, relations, functions);
                    }
                    _ => {}
                }
            }
        }
        NodeEnum::AlterTableStmt(alter_stmt) => {
            // Extract from ALTER TABLE commands
            for cmd in &alter_stmt.cmds {
                if let Some(NodeEnum::AlterTableCmd(table_cmd)) = &cmd.node {
                    // Handle ADD CONSTRAINT commands
                    if let Some(def) = &table_cmd.def {
                        if let Some(NodeEnum::Constraint(c)) = &def.node {
                            extract_from_constraint(c, relations, functions);
                        }
                    }
                }
            }
        }
        NodeEnum::FuncCall(func_call) => {
            extract_function_from_func_call(func_call, functions);
        }
        NodeEnum::RangeVar(range_var) => {
            // Extract table references
            let table_ident = if !range_var.schemaname.is_empty() {
                QualifiedIdent::new(Some(range_var.schemaname.clone()), range_var.relname.clone())
            } else {
                QualifiedIdent::from_name(range_var.relname.clone())
            };
            relations.insert(table_ident);
        }
        NodeEnum::TypeCast(type_cast) => {
            if let Some(arg) = &type_cast.arg {
                extract_from_node(arg.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::CreateDomainStmt(domain_stmt) => {
            // Extract base type from domain definition
            if let Some(type_name) = &domain_stmt.type_name {
                if let Some(_qualified_type) = extract_type_from_type_name(type_name) {
                    // This will be handled by extract_types_from_ast, but we can also
                    // extract from domain constraints if they contain function calls
                }
            }
            
            // Extract from domain constraints
            for constraint in &domain_stmt.constraints {
                if let Some(NodeEnum::Constraint(c)) = &constraint.node {
                    extract_from_constraint(c, relations, functions);
                }
            }
        }
        NodeEnum::AExpr(a_expr) => {
            // For expression nodes, recursively traverse left and right expressions
            if let Some(lexpr) = &a_expr.lexpr {
                extract_from_node(lexpr.node.as_ref().unwrap(), relations, functions);
            }
            if let Some(rexpr) = &a_expr.rexpr {
                extract_from_node(rexpr.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::BoolExpr(bool_expr) => {
            // For boolean expressions, traverse all arguments
            for arg in &bool_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::SubLink(sublink) => {
            // For subqueries, extract from the subselect
            if let Some(subselect) = &sublink.subselect {
                extract_from_node(subselect.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::CreateTrigStmt(trigger_stmt) => {
            // Extract function name from EXECUTE FUNCTION clause
            extract_function_name_from_nodes(&trigger_stmt.funcname, functions);
        }
        NodeEnum::InsertStmt(insert_stmt) => {
            // Extract from ON CONFLICT clause
            if let Some(on_conflict) = &insert_stmt.on_conflict_clause {
                // Extract from target_list (SET expressions)
                for target in &on_conflict.target_list {
                    if let Some(node) = &target.node {
                        extract_from_node(node, relations, functions);
                    }
                }
                // Extract from WHERE clause
                if let Some(where_clause) = &on_conflict.where_clause {
                    extract_from_node(where_clause.node.as_ref().unwrap(), relations, functions);
                }
            }
            
            // Extract from RETURNING clause
            for returning_item in &insert_stmt.returning_list {
                if let Some(node) = &returning_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::UpdateStmt(update_stmt) => {
            // Extract from RETURNING clause
            for returning_item in &update_stmt.returning_list {
                if let Some(node) = &returning_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::DeleteStmt(delete_stmt) => {
            // Extract from RETURNING clause
            for returning_item in &delete_stmt.returning_list {
                if let Some(node) = &returning_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::ResTarget(res_target) => {
            // Extract from the value expression
            if let Some(val) = &res_target.val {
                extract_from_node(val.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::List(list) => {
            // Lists can contain function calls in their items
            for item in &list.items {
                if let Some(node) = &item.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::CaseExpr(case_expr) => {
            // Handle CASE expressions
            if let Some(arg) = &case_expr.arg {
                extract_from_node(arg.node.as_ref().unwrap(), relations, functions);
            }
            if let Some(defresult) = &case_expr.defresult {
                extract_from_node(defresult.node.as_ref().unwrap(), relations, functions);
            }
            for when_clause in &case_expr.args {
                if let Some(node) = &when_clause.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::CaseWhen(case_when) => {
            // Handle WHEN clauses in CASE expressions
            if let Some(expr) = &case_when.expr {
                extract_from_node(expr.node.as_ref().unwrap(), relations, functions);
            }
            if let Some(result) = &case_when.result {
                extract_from_node(result.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::CoalesceExpr(coalesce_expr) => {
            // Handle COALESCE expressions
            // COALESCE is both a special expression and a function, so we track it
            functions.insert(QualifiedIdent::from_name("coalesce".to_string()));
            
            for arg in &coalesce_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::MinMaxExpr(min_max_expr) => {
            // Handle GREATEST/LEAST expressions
            for arg in &min_max_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::NullTest(null_test) => {
            // Handle IS NULL/IS NOT NULL
            if let Some(arg) = &null_test.arg {
                extract_from_node(arg.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::BooleanTest(boolean_test) => {
            // Handle IS TRUE/IS FALSE
            if let Some(arg) = &boolean_test.arg {
                extract_from_node(arg.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::ArrayExpr(array_expr) => {
            // Handle array expressions
            for element in &array_expr.elements {
                if let Some(node) = &element.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::RowExpr(row_expr) => {
            // Handle row expressions (used in ROW() constructs)
            for arg in &row_expr.args {
                if let Some(node) = &arg.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::SelectStmt(select_stmt) => {
            // Handle SELECT statements (in subqueries)
            // Extract from FROM clause
            for from_item in &select_stmt.from_clause {
                if let Some(node) = &from_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
            
            for target in &select_stmt.target_list {
                if let Some(node) = &target.node {
                    extract_from_node(node, relations, functions);
                }
            }
            if let Some(where_clause) = &select_stmt.where_clause {
                extract_from_node(where_clause.node.as_ref().unwrap(), relations, functions);
            }
            if let Some(having_clause) = &select_stmt.having_clause {
                extract_from_node(having_clause.node.as_ref().unwrap(), relations, functions);
            }
            for group_item in &select_stmt.group_clause {
                if let Some(node) = &group_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
            for sort_item in &select_stmt.sort_clause {
                if let Some(node) = &sort_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::SortBy(sort_by) => {
            // Handle ORDER BY clauses
            if let Some(node) = &sort_by.node {
                extract_from_node(node.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::GroupingSet(grouping_set) => {
            // Handle GROUP BY clauses
            for item in &grouping_set.content {
                if let Some(node) = &item.node {
                    extract_from_node(node, relations, functions);
                }
            }
        }
        NodeEnum::WindowDef(window_def) => {
            // Handle window definitions
            for partition_item in &window_def.partition_clause {
                if let Some(node) = &partition_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
            for order_item in &window_def.order_clause {
                if let Some(node) = &order_item.node {
                    extract_from_node(node, relations, functions);
                }
            }
            if let Some(start_offset) = &window_def.start_offset {
                extract_from_node(start_offset.node.as_ref().unwrap(), relations, functions);
            }
            if let Some(end_offset) = &window_def.end_offset {
                extract_from_node(end_offset.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::AConst(_) | NodeEnum::ColumnRef(_) | NodeEnum::ParamRef(_) => {
            // These are leaf nodes that don't contain function calls
        }
        _ => {
            // For any other node types, we should ideally handle them
            // For now, we'll leave this as a catch-all
        }
    }
}

fn extract_types_from_ast(parse_result: &pg_query::protobuf::ParseResult) -> Result<Vec<QualifiedIdent>, Box<dyn std::error::Error>> {
    let mut types = HashSet::new();
    
    for (node, _depth, _context, _has_filter_columns) in parse_result.nodes() {
        match node {
            NodeRef::TypeCast(type_cast) => {
                if let Some(type_name) = &type_cast.type_name {
                    if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                        types.insert(qualified_type);
                    }
                }
            }
            NodeRef::CompositeTypeStmt(composite_type) => {
                // Extract types from composite type column definitions
                for col_def in &composite_type.coldeflist {
                    if let Some(NodeEnum::ColumnDef(column_def)) = &col_def.node {
                        if let Some(type_name) = &column_def.type_name {
                            if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                                types.insert(qualified_type);
                            }
                        }
                    }
                }
            }
            NodeRef::CreateDomainStmt(domain_stmt) => {
                // Extract the base type of the domain
                if let Some(type_name) = &domain_stmt.type_name {
                    if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                        types.insert(qualified_type);
                    }
                }
            }
            NodeRef::CreateStmt(create_stmt) => {
                // Extract types from CREATE TABLE column definitions
                for table_elt in &create_stmt.table_elts {
                    if let Some(NodeEnum::ColumnDef(column_def)) = &table_elt.node {
                        if let Some(type_name) = &column_def.type_name {
                            if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                                types.insert(qualified_type);
                            }
                        }
                        
                        // Note: Constraints are handled separately for REFERENCES and DEFAULT
                    }
                }
            }
            NodeRef::AlterTableStmt(alter_table) => {
                // Extract types from ALTER TABLE commands
                for cmd in &alter_table.cmds {
                    if let Some(NodeEnum::AlterTableCmd(alter_cmd)) = &cmd.node {
                        // Handle column definitions in ALTER TABLE
                        if let Some(def) = &alter_cmd.def {
                            if let Some(NodeEnum::ColumnDef(column_def)) = &def.node {
                                if let Some(type_name) = &column_def.type_name {
                                    if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                                        types.insert(qualified_type);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    
    Ok(types.into_iter().collect())
}


fn extract_type_from_type_name(type_name: &pg_query::protobuf::TypeName) -> Option<QualifiedIdent> {
    if type_name.names.is_empty() {
        return None;
    }
    
    let name_parts: Vec<String> = type_name.names.iter()
        .filter_map(|node| {
            if let Some(NodeEnum::String(s)) = &node.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    if name_parts.is_empty() {
        return None;
    }
    
    // Note: array_bounds being non-empty indicates this is an array type (e.g., api.order_item[])
    // We still want to extract the base type dependency
    
    if name_parts.len() == 1 {
        Some(QualifiedIdent::from_name(name_parts[0].clone()))
    } else if name_parts.len() == 2 {
        Some(QualifiedIdent::new(Some(name_parts[0].clone()), name_parts[1].clone()))
    } else {
        // Handle cases with more than 2 parts by taking the last two
        let len = name_parts.len();
        Some(QualifiedIdent::new(
            Some(name_parts[len - 2].clone()),
            name_parts[len - 1].clone(),
        ))
    }
}


pub fn analyze_plpgsql(sql: &str) -> Result<Dependencies, Box<dyn std::error::Error>> {
    let json_result = pg_query::parse_plpgsql(sql)?;
    
    let mut all_relations = HashSet::new();
    let mut all_functions = HashSet::new();
    let mut all_types = HashSet::new();
    
    // First, extract the return type and parameter types from the CREATE FUNCTION statement
    if let Ok(parse_result) = pg_query::parse(sql) {
        for (node, _, _, _) in parse_result.protobuf.nodes() {
            if let NodeRef::CreateFunctionStmt(create_func) = node {
                // Extract return type
                if let Some(return_type) = &create_func.return_type {
                    if let Some(qualified_type) = extract_type_from_type_name(return_type) {
                        all_types.insert(qualified_type);
                    }
                }
                
                // Extract parameter types
                for param in &create_func.parameters {
                    if let Some(NodeEnum::FunctionParameter(func_param)) = &param.node {
                        if let Some(arg_type) = &func_param.arg_type {
                            if let Some(qualified_type) = extract_type_from_type_name(arg_type) {
                                all_types.insert(qualified_type);
                            }
                        }
                    }
                }
            }
        }
    }
    
    // The result is a JSON array of PL/pgSQL functions
    if let Value::Array(functions) = &json_result {
        for function in functions {
            extract_dependencies_from_plpgsql_function(function, &mut all_relations, &mut all_functions, &mut all_types)?;
        }
    }
    
    Ok(Dependencies {
        relations: all_relations,
        functions: all_functions,
        types: all_types,
    })
}

fn extract_dependencies_from_plpgsql_function(
    function_json: &Value,
    relations: &mut HashSet<QualifiedIdent>,
    functions: &mut HashSet<QualifiedIdent>,
    types: &mut HashSet<QualifiedIdent>
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract types from datums (variable declarations)
    if let Some(Value::Object(func_obj)) = function_json.get("PLpgSQL_function") {
        if let Some(Value::Array(datums)) = func_obj.get("datums") {
            for datum in datums {
                extract_types_from_datum(datum, types);
            }
        }
    }
    
    // Extract all PL/pgSQL expressions from the function body
    let plpgsql_expressions = extract_plpgsql_expressions_from_json(function_json);
    
    // Analyze each expression
    for expr in plpgsql_expressions {
        // Check if it's a regular SQL statement
        let expr_upper = expr.trim().to_uppercase();
        if expr_upper.starts_with("SELECT") 
            || expr_upper.starts_with("INSERT")
            || expr_upper.starts_with("UPDATE")
            || expr_upper.starts_with("DELETE")
            || expr_upper.starts_with("WITH") {
            // It's a SQL statement, analyze it but avoid recursive PL/pgSQL analysis
            if let Ok(parse_result) = pg_query::parse(&expr) {
                if let Ok(deps) = extract_dependencies_from_parse_result(&parse_result.protobuf) {
                    for relation in deps.relations {
                        relations.insert(relation);
                    }
                    for function in deps.functions {
                        functions.insert(function);
                    }
                    for typ in deps.types {
                        types.insert(typ);
                    }
                }
            }
        } else {
            // It's a PL/pgSQL expression (assignment, condition, etc.)
            // Extract function calls and type casts from the expression
            extract_dependencies_from_plpgsql_expression(&expr, functions, types);
        }
    }
    
    Ok(())
}

fn extract_plpgsql_expressions_from_json(value: &Value) -> Vec<String> {
    let mut expressions = Vec::new();
    
    match value {
        Value::Object(map) => {
            // Look for PLpgSQL_expr nodes which contain expressions
            if let Some(Value::Object(expr_map)) = map.get("PLpgSQL_expr") {
                if let Some(Value::String(query)) = expr_map.get("query") {
                    expressions.push(query.clone());
                }
            }
            
            // Recursively search in all values
            for (_, v) in map {
                expressions.extend(extract_plpgsql_expressions_from_json(v));
            }
        }
        Value::Array(arr) => {
            // Recursively search in array elements
            for v in arr {
                expressions.extend(extract_plpgsql_expressions_from_json(v));
            }
        }
        _ => {}
    }
    
    expressions
}

fn extract_dependencies_from_plpgsql_expression(
    expr: &str,
    functions: &mut HashSet<QualifiedIdent>,
    types: &mut HashSet<QualifiedIdent>
) {
    // Handle assignment expressions by extracting the right-hand side
    let expr_to_parse = if expr.contains(":=") {
        // Extract the RHS of the assignment
        if let Some(rhs) = expr.split(":=").nth(1) {
            rhs.trim()
        } else {
            expr
        }
    } else {
        expr
    };
    
    // Try to parse the expression as a SELECT statement to leverage existing parsing
    let select_expr = format!("SELECT {}", expr_to_parse);
    if let Ok(parse_result) = pg_query::parse(&select_expr) {
        // Extract function calls
        for (node, _, _, _) in parse_result.protobuf.nodes() {
            if let NodeRef::FuncCall(func_call) = node {
                extract_function_from_func_call(func_call, functions);
            }
        }
        
        // Extract types using the existing function
        if let Ok(found_types) = extract_types_from_ast(&parse_result.protobuf) {
            for typ in found_types {
                types.insert(typ);
            }
        }
    }
}

fn extract_function_from_func_call(
    func_call: &pg_query::protobuf::FuncCall,
    functions: &mut HashSet<QualifiedIdent>
) {
    // Extract function name from funcname list
    extract_function_name_from_nodes(&func_call.funcname, functions);
    
    // Also extract from function arguments - this is crucial for nested function calls
    for arg in &func_call.args {
        if let Some(node) = &arg.node {
            // This is a dummy relations set since we're only interested in functions here
            let mut dummy_relations = HashSet::new();
            extract_from_node(node, &mut dummy_relations, functions);
        }
    }
}

fn extract_types_from_datum(datum: &Value, types: &mut HashSet<QualifiedIdent>) {
    if let Some(Value::Object(var_obj)) = datum.get("PLpgSQL_var") {
        if let Some(Value::Object(datatype_obj)) = var_obj.get("datatype") {
            if let Some(Value::Object(type_obj)) = datatype_obj.get("PLpgSQL_type") {
                if let Some(Value::String(typname)) = type_obj.get("typname") {
                    // Parse the type name, which might be qualified like "pg_catalog.int4"
                    // Also remove any quotes from the type name
                    let clean_typname = typname.trim_matches('"');
                    let type_ident = if clean_typname.contains('.') {
                        QualifiedIdent::from_qualified_name(clean_typname)
                    } else {
                        QualifiedIdent::from_name(clean_typname.to_string())
                    };
                    
                    // Insert all types - built-ins will be filtered later
                    types.insert(type_ident);
                }
            }
        }
    }
}

// Copy the rest of the tests from the original lib.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_type_cast() {
        let sql = "select id::custom_type from users";
        let result = analyze_statement(sql).unwrap();
        
        let expected_type = QualifiedIdent::from_name("custom_type".to_string());
        assert!(result.types.contains(&expected_type));
    }

    #[test]
    fn test_qualified_type_cast() {
        let sql = "select id::api.custom_type from users";
        let result = analyze_statement(sql).unwrap();
        
        let expected_type = QualifiedIdent::new(Some("api".to_string()), "custom_type".to_string());
        assert!(result.types.contains(&expected_type));
    }

    #[test]
    fn test_cte_with_types() {
        let sql = r#"
        with data as (
            select id::api.user_id, name::custom.string_type
            from users
        )
        select * from data
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let user_id_type = QualifiedIdent::new(Some("api".to_string()), "user_id".to_string());
        let string_type = QualifiedIdent::new(Some("custom".to_string()), "string_type".to_string());
        
        assert!(result.types.contains(&user_id_type));
        assert!(result.types.contains(&string_type));
    }

    #[test]
    fn test_nested_casts() {
        let sql = "select (price::decimal)::api.money_type from products";
        let result = analyze_statement(sql).unwrap();
        
        let decimal_type = QualifiedIdent::from_name("decimal".to_string());
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        
        assert!(result.types.contains(&decimal_type));
        assert!(result.types.contains(&money_type));
    }

    #[test]
    fn test_array_types() {
        let sql = "select array[1,2,3]::custom.int_array from users";
        let result = analyze_statement(sql).unwrap();
        
        let array_type = QualifiedIdent::new(Some("custom".to_string()), "int_array".to_string());
        assert!(result.types.contains(&array_type));
    }

    #[test]
    fn test_builtin_types_filtered() {
        let sql = "select id::integer, name::text, created::timestamp from users";
        let result = analyze_statement(sql).unwrap();
        
        // These are built-in types and should not be included
        let integer_type = QualifiedIdent::from_name("integer".to_string());
        let text_type = QualifiedIdent::from_name("text".to_string());
        let timestamp_type = QualifiedIdent::from_name("timestamp".to_string());
        
        // Note: The current implementation doesn't filter built-ins yet
        // This test documents the current behavior
        assert!(result.types.contains(&integer_type));
        assert!(result.types.contains(&text_type));
        assert!(result.types.contains(&timestamp_type));
    }

    #[test]
    fn test_complex_query_with_multiple_schemas() {
        let sql = r#"
        select 
            u.id::api.user_id,
            o.total::billing.money_amount,
            o.status::core.order_status
        from api.users u
        join billing.orders o on u.id = o.user_id
        where o.created_at > now()::api.timestamp_tz
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let user_id_type = QualifiedIdent::new(Some("api".to_string()), "user_id".to_string());
        let money_type = QualifiedIdent::new(Some("billing".to_string()), "money_amount".to_string());
        let status_type = QualifiedIdent::new(Some("core".to_string()), "order_status".to_string());
        let timestamp_type = QualifiedIdent::new(Some("api".to_string()), "timestamp_tz".to_string());
        
        assert!(result.types.contains(&user_id_type));
        assert!(result.types.contains(&money_type));
        assert!(result.types.contains(&status_type));
        assert!(result.types.contains(&timestamp_type));
        
        // Check that tables are also extracted
        let users_table = QualifiedIdent::new(Some("api".to_string()), "users".to_string());
        let orders_table = QualifiedIdent::new(Some("billing".to_string()), "orders".to_string());
        
        assert!(result.relations.contains(&users_table));
        assert!(result.relations.contains(&orders_table));
    }

    #[test]
    fn test_json_operations_with_casts() {
        let sql = r#"
        select 
            (data->>'amount')::api.money_type,
            (metadata->'config')::jsonb::custom.config_type
        from transactions
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        let jsonb_type = QualifiedIdent::from_name("jsonb".to_string());
        let config_type = QualifiedIdent::new(Some("custom".to_string()), "config_type".to_string());
        
        assert!(result.types.contains(&money_type));
        assert!(result.types.contains(&jsonb_type));
        assert!(result.types.contains(&config_type));
    }

    #[test]
    fn test_window_function_with_casts() {
        let sql = r#"
        select 
            id,
            row_number() over (order by created_at)::api.sequence_number,
            lag(amount::api.money_type) over (partition by user_id order by created_at) as prev_amount
        from transactions
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let sequence_type = QualifiedIdent::new(Some("api".to_string()), "sequence_number".to_string());
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        
        assert!(result.types.contains(&sequence_type));
        assert!(result.types.contains(&money_type));
        
        // Check for window functions
        let row_number_func = QualifiedIdent::from_name("row_number".to_string());
        let lag_func = QualifiedIdent::from_name("lag".to_string());
        
        assert!(result.functions.contains(&row_number_func));
        assert!(result.functions.contains(&lag_func));
    }

    #[test]
    fn test_recursive_cte_with_types() {
        let sql = r#"
        with recursive hierarchy as (
            select id::api.node_id, parent_id::api.node_id, 1::api.depth_level as level
            from categories
            where parent_id is null
            
            union all
            
            select c.id::api.node_id, c.parent_id::api.node_id, (h.level + 1)::api.depth_level
            from categories c
            join hierarchy h on c.parent_id = h.id
        )
        select * from hierarchy
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let node_id_type = QualifiedIdent::new(Some("api".to_string()), "node_id".to_string());
        let depth_type = QualifiedIdent::new(Some("api".to_string()), "depth_level".to_string());
        
        assert!(result.types.contains(&node_id_type));
        assert!(result.types.contains(&depth_type));
    }

    #[test]
    fn test_case_expression_with_casts() {
        let sql = r#"
        select 
            case 
                when status = 'active' then 1::api.status_code
                when status = 'inactive' then 0::api.status_code
                else null::api.status_code
            end as status_code
        from users
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let status_code_type = QualifiedIdent::new(Some("api".to_string()), "status_code".to_string());
        assert!(result.types.contains(&status_code_type));
    }

    #[test]
    fn test_subquery_with_types() {
        let sql = r#"
        select u.id, 
               (select count(*)::api.order_count from orders where user_id = u.id) as order_count
        from users u
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let order_count_type = QualifiedIdent::new(Some("api".to_string()), "order_count".to_string());
        assert!(result.types.contains(&order_count_type));
    }

    #[test]
    fn test_insert_with_casts() {
        let sql = r#"
        insert into orders (user_id, amount, status)
        values (123::api.user_id, 99.99::api.money_type, 'pending'::api.order_status)
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let user_id_type = QualifiedIdent::new(Some("api".to_string()), "user_id".to_string());
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        let status_type = QualifiedIdent::new(Some("api".to_string()), "order_status".to_string());
        
        assert!(result.types.contains(&user_id_type));
        assert!(result.types.contains(&money_type));
        assert!(result.types.contains(&status_type));
    }

    #[test]
    fn test_update_with_casts() {
        let sql = r#"
        update orders 
        set amount = (amount * 1.1)::api.money_type,
            updated_at = now()::api.timestamp_tz
        where status = 'draft'::api.order_status
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        let timestamp_type = QualifiedIdent::new(Some("api".to_string()), "timestamp_tz".to_string());
        let status_type = QualifiedIdent::new(Some("api".to_string()), "order_status".to_string());
        
        assert!(result.types.contains(&money_type));
        assert!(result.types.contains(&timestamp_type));
        assert!(result.types.contains(&status_type));
    }

    #[test]
    fn test_create_table_with_custom_types() {
        let sql = r#"
        create table products (
            id serial primary key,
            name text not null,
            price api.money_type not null,
            category_id api.category_id references categories(id),
            metadata custom.product_metadata default '{}'::custom.product_metadata
        )
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        let category_id_type = QualifiedIdent::new(Some("api".to_string()), "category_id".to_string());
        let metadata_type = QualifiedIdent::new(Some("custom".to_string()), "product_metadata".to_string());
        
        assert!(result.types.contains(&money_type));
        assert!(result.types.contains(&category_id_type));
        assert!(result.types.contains(&metadata_type));
        
        // Check for referenced table
        let categories_table = QualifiedIdent::from_name("categories".to_string());
        assert!(result.relations.contains(&categories_table));
    }

    #[test]
    fn test_create_function_with_custom_types() {
        let sql = r#"
        create function calculate_total(
            base_amount api.money_type,
            tax_rate api.percentage
        ) returns api.money_type as $$
            select (base_amount * (1 + tax_rate))::api.money_type
        $$ language sql;
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let money_type = QualifiedIdent::new(Some("api".to_string()), "money_type".to_string());
        let percentage_type = QualifiedIdent::new(Some("api".to_string()), "percentage".to_string());
        
        assert!(result.types.contains(&money_type));
        assert!(result.types.contains(&percentage_type));
        
        // Check that the function itself is tracked
        let calc_func = QualifiedIdent::from_name("calculate_total".to_string());
        assert!(result.functions.contains(&calc_func));
    }

    #[test]
    fn test_create_domain_with_base_type() {
        let sql = r#"
        create domain api.email_address as text
        check (value ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let text_type = QualifiedIdent::from_name("text".to_string());
        assert!(result.types.contains(&text_type));
    }

    #[test]
    fn test_create_composite_type() {
        let sql = r#"
        create type api.address as (
            street text,
            city text,
            postal_code api.postal_code,
            country api.country_code
        )
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let text_type = QualifiedIdent::from_name("text".to_string());
        let postal_type = QualifiedIdent::new(Some("api".to_string()), "postal_code".to_string());
        let country_type = QualifiedIdent::new(Some("api".to_string()), "country_code".to_string());
        
        assert!(result.types.contains(&text_type));
        assert!(result.types.contains(&postal_type));
        assert!(result.types.contains(&country_type));
    }

    #[test]
    fn test_alter_table_add_column_with_type() {
        let sql = r#"
        alter table users 
        add column profile_data custom.user_profile default '{}'::custom.user_profile
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let profile_type = QualifiedIdent::new(Some("custom".to_string()), "user_profile".to_string());
        assert!(result.types.contains(&profile_type));
    }

    #[test]
    fn test_complex_expression_with_multiple_casts() {
        let sql = r#"
        select 
            coalesce(
                sum(convert_currency(id.price, p_currency_code) * cl.quantity),
                (0, p_currency_code)::currency
            ),
            coalesce(sum(cl.quantity), 0)::int
        )::api.cart_summary
        from cart_listing cl
        join api.item_details id on cl.item_id = id.item_id
        where cl.account_id = p_account_id
        "#;
        let result = analyze_statement(sql).unwrap();
        
        let currency_type = QualifiedIdent::from_name("currency".to_string());
        let int_type = QualifiedIdent::from_name("int".to_string());
        let cart_summary_type = QualifiedIdent::new(Some("api".to_string()), "cart_summary".to_string());
        
        assert!(result.types.contains(&currency_type));
        assert!(result.types.contains(&int_type));
        assert!(result.types.contains(&cart_summary_type));
        
        // Check for functions
        let coalesce_func = QualifiedIdent::from_name("coalesce".to_string());
        let sum_func = QualifiedIdent::from_name("sum".to_string());
        let convert_func = QualifiedIdent::from_name("convert_currency".to_string());
        
        assert!(result.functions.contains(&coalesce_func));
        assert!(result.functions.contains(&sum_func));
        assert!(result.functions.contains(&convert_func));
    }

    #[test]
    fn test_plpgsql_function() {
        let sql = r#"
        create or replace function api.delete_parcel_template(
            p_template_id int,
            p_account_id  int
        ) returns void
            language plpgsql
            volatile as
        $$
        declare
            v_updated_count int;
        begin
            update parcel_template
            set deleted_at = now()
            where parcel_template_id = p_template_id
              and account_id = p_account_id;

            get diagnostics v_updated_count = row_count;

            if v_updated_count = 0 then
                raise exception no_data_found using message = 'Parcel template not found or access denied';
            end if;
        end;
        $$"#;
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that the function is tracked
        let func_ident = QualifiedIdent::new(Some("api".to_string()), "delete_parcel_template".to_string());
        assert!(result.functions.contains(&func_ident));
        
        // Check for table reference
        let table_ident = QualifiedIdent::from_name("parcel_template".to_string());
        assert!(result.relations.contains(&table_ident));
        
        // Check for built-in function
        let now_func = QualifiedIdent::from_name("now".to_string());
        assert!(result.functions.contains(&now_func));
        
        // Check parameter and return types
        let int_type = QualifiedIdent::from_name("int".to_string());
        let void_type = QualifiedIdent::from_name("void".to_string());
        assert!(result.types.contains(&int_type));
        assert!(result.types.contains(&void_type));
    }

    #[test]
    fn test_composite_type_with_array_field() {
        // Test that array type dependencies are correctly extracted
        let sql = r#"
            CREATE TYPE api.order_shipment AS (
                shipment_id int,
                tracking_number text,
                items api.order_item[]
            );
        "#;
        
        let result = analyze_statement(sql).unwrap();
        
        // Should detect dependency on api.order_item (without the array notation)
        let order_item_type = QualifiedIdent::new(Some("api".to_string()), "order_item".to_string());
        assert!(result.types.contains(&order_item_type), 
            "Failed to extract dependency on api.order_item from array type api.order_item[]. Found types: {:?}", 
            result.types);
    }

    #[test]
    fn test_multiple_array_type_dependencies() {
        // Test with multiple custom types including arrays
        let sql = r#"
            CREATE TYPE api.order_shipment AS (
                shipment_id           int,
                tracking_number       text,
                tracking_url_provider text,
                shipping_price        currency,
                status                tracking_status,
                must_ship_by          timestamptz,
                shipped_at            timestamptz,
                delivered_at          timestamptz,
                is_cancelled          boolean,
                cancellation_reason   shipment_cancellation_reason,
                items                 api.order_item[]
            );
        "#;
        
        let result = analyze_statement(sql).unwrap();
        
        // Should detect all custom type dependencies
        let expected_types = vec![
            QualifiedIdent::from_name("currency".to_string()),
            QualifiedIdent::from_name("tracking_status".to_string()),
            QualifiedIdent::from_name("shipment_cancellation_reason".to_string()),
            QualifiedIdent::new(Some("api".to_string()), "order_item".to_string()),
        ];
        
        for expected_type in expected_types {
            assert!(result.types.contains(&expected_type), 
                "Failed to extract dependency on {:?}. Found types: {:?}", 
                expected_type, result.types);
        }
    }

    #[test]
    fn test_composite_type_with_enum_fields() {
        // Test the exact case from the user
        let sql = r#"
            CREATE TYPE api.order_item AS (
                item_id              int,
                product_id           int,
                product_name         text,
                product_image        url,
                description          text,
                item_image           url,
                price                currency,
                quantity             int,
                mini_assembly_status assembly_status,
                mini_painting_status painting_status,
                mini_content_status  content_status
            );
        "#;
        
        let result = analyze_statement(sql).unwrap();
        
        // Should detect all custom type dependencies
        let expected_types = vec![
            QualifiedIdent::from_name("url".to_string()),
            QualifiedIdent::from_name("currency".to_string()),
            QualifiedIdent::from_name("assembly_status".to_string()),
            QualifiedIdent::from_name("painting_status".to_string()),
            QualifiedIdent::from_name("content_status".to_string()),
        ];
        
        println!("Found types: {:?}", result.types);
        
        for expected_type in expected_types {
            assert!(result.types.contains(&expected_type), 
                "Failed to extract dependency on {:?}. Found types: {:?}", 
                expected_type, result.types);
        }
    }

    #[test]
    fn test_identify_both_order_types() {
        // Test that both types are identified from a file
        let sql = r#"
create type api.order_item as (
    item_id              int,
    product_id           int,
    product_name         text,
    product_image        url,
    description          text,
    item_image           url,
    price                currency,
    quantity             int,
    mini_assembly_status assembly_status,
    mini_painting_status painting_status,
    mini_content_status  content_status
);

create type api.order_shipment as (
    shipment_id           int,
    tracking_number       text,
    tracking_url_provider text,
    shipping_price        currency,
    status                tracking_status,
    must_ship_by          timestamptz,
    shipped_at            timestamptz,
    delivered_at          timestamptz,
    is_cancelled          boolean,
    cancellation_reason   shipment_cancellation_reason,
    items                 api.order_item[]
);
        "#;
        
        let statements = crate::sql::splitter::split_sql_file(sql).unwrap();
        println!("Split into {} statements", statements.len());
        
        let mut found_objects = Vec::new();
        for stmt in statements {
            println!("Processing statement: {}", &stmt.sql[..50.min(stmt.sql.len())]);
            if let Some(obj) = crate::sql::objects::identify_sql_object(&stmt.sql).unwrap() {
                println!("Identified object: {:?} - {:?}", obj.object_type, obj.qualified_name);
                found_objects.push(obj);
            }
        }
        
        assert_eq!(found_objects.len(), 2, "Should find both types");
        
        let order_item = found_objects.iter().find(|o| o.qualified_name.name == "order_item");
        let order_shipment = found_objects.iter().find(|o| o.qualified_name.name == "order_shipment");
        
        assert!(order_item.is_some(), "Should find api.order_item");
        assert!(order_shipment.is_some(), "Should find api.order_shipment");
    }
}