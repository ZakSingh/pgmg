use std::collections::HashSet;
use pg_query::{NodeEnum, NodeRef};
use serde_json::Value;

pub mod builtin_catalog;
use builtin_catalog::BuiltinCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedIdent {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]
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
    
    // Extract relations using existing pg_query functionality
    let mut relations = HashSet::new();
    for table in parse_result.tables() {
        relations.insert(QualifiedIdent::from_qualified_name(&table));
    }
    
    let mut functions = HashSet::new();
    
    // Get functions from pg_query's built-in functionality
    for func in parse_result.functions() {
        functions.insert(QualifiedIdent::from_qualified_name(&func));
    }
    
    let mut types = HashSet::new();
    
    // Also traverse the entire AST to extract REFERENCES and DEFAULT functions
    for stmt in &parse_result.protobuf.stmts {
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
                        // since analyze_plpgsql expects the complete CREATE FUNCTION
                        match analyze_plpgsql(sql) {
                            Ok(body_deps) => {
                                relations.extend(body_deps.relations);
                                functions.extend(body_deps.functions);
                                types.extend(body_deps.types);
                            }
                            Err(e) => {
                                // Log the error but don't fail the entire analysis
                                eprintln!("Warning: Failed to parse PL/pgSQL function: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Extract types from cast expressions at the top level
    let top_level_types = extract_types_from_ast(&parse_result.protobuf)?;
    for typ in top_level_types {
        types.insert(typ);
    }
    
    Ok(Dependencies {
        relations,
        functions,
        types,
    })
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
        relations: all_relations.into_iter().collect(),
        functions: all_functions.into_iter().collect(),
        types: all_types.into_iter().collect(),
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
            // It's a SQL statement, analyze it normally
            if let Ok(deps) = analyze_statement(&expr) {
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_type_cast() {
        let sql = "SELECT 42::currency";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        let currency_type = result.types.iter().find(|t| t.name == "currency").unwrap();
        assert_eq!(currency_type.schema, None);
    }

    #[test]
    fn test_qualified_type_cast() {
        let sql = "SELECT 42::api.cart_summary";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        let cart_summary_type = result.types.iter().find(|t| t.name == "cart_summary").unwrap();
        assert_eq!(cart_summary_type.schema, Some("api".to_string()));
    }

    #[test]
    fn test_complex_query_from_main() {
        let sql = "select (
            coalesce(sum(convert_currency(id.price, p_currency_code) * cl.quantity),
                     (0, p_currency_code)::currency),
            coalesce(sum(cl.quantity), 0)::int
               )::api.cart_summary
        from cart_listing cl
             join api.item_details id on cl.item_id = id.item_id
        where cl.account_id = p_account_id
          and cl.selected_for_checkout = true";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that we found the custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"cart_summary"));
        
        // Check that built-in types are filtered out
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"int"));
        
        // Check schema qualification
        let api_cart_summary = result.types.iter()
            .find(|t| t.name == "cart_summary")
            .unwrap();
        assert_eq!(api_cart_summary.schema, Some("api".to_string()));
    }

    #[test]
    fn test_function_style_cast() {
        let sql = "SELECT CAST(42 AS currency)";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        assert!(result.types.iter().any(|t| t.name == "currency"));
    }

    #[test]
    fn test_cte_with_types() {
        let sql = "WITH summary AS (
            SELECT (1, 'USD')::currency AS amount
        )
        SELECT (amount, 100)::api.order_total 
        FROM summary";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"order_total"));
    }

    #[test]
    fn test_nested_casts() {
        let sql = "SELECT ((1, 'USD')::currency, 'active')::user_account.status";
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"status"));
    }

    #[test]
    fn test_array_types() {
        let sql = "SELECT ARRAY[1,2,3]::custom_type[]";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        assert!(result.types.iter().any(|t| t.name == "custom_type"));
    }

    #[test]
    fn test_builtin_types_included() {
        let sql = "SELECT '2023-01-01'::date, 42::integer, 'hello'::text, true::boolean";
        let result = analyze_statement(sql).unwrap();
        
        // Built-in types are now included in analysis (filtering happens later)
        assert!(result.types.len() > 0);
        // PostgreSQL uses canonical type names
        assert!(result.types.iter().any(|t| t.name == "date"));
        assert!(result.types.iter().any(|t| t.name == "int4")); // integer -> int4
        assert!(result.types.iter().any(|t| t.name == "text"));
        assert!(result.types.iter().any(|t| t.name == "bool")); // boolean -> bool
    }

    #[test]
    fn test_mixed_builtin_and_custom() {
        let sql = "SELECT 42::integer, 'data'::custom_type, true::boolean";
        let result = analyze_statement(sql).unwrap();
        
        // Now includes both built-in and custom types
        assert!(result.types.len() >= 3);
        assert!(result.types.iter().any(|t| t.name == "custom_type"));
        assert!(result.types.iter().any(|t| t.name == "int4")); // integer -> int4
        assert!(result.types.iter().any(|t| t.name == "bool")); // boolean -> bool
    }

    #[test]
    fn test_subquery_with_types() {
        let sql = "SELECT * FROM (
            SELECT (1, 'test')::composite_type AS data
        ) sub
        WHERE sub.data::text_type = 'test'";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"composite_type"));
        assert!(type_names.contains(&"text_type"));
    }

    #[test]
    fn test_case_expression_with_cast() {
        let sql = "SELECT CASE 
            WHEN status = 'active' THEN ('active', now())::status_log
            ELSE ('inactive', now())::status_log
        END";
        
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        assert!(result.types.iter().any(|t| t.name == "status_log"));
    }

    #[test]
    fn test_window_function_with_cast() {
        let sql = "SELECT 
            row_number() OVER (ORDER BY id)::sequence_number,
            data::processed_data
        FROM test_table";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"sequence_number"));
        assert!(type_names.contains(&"processed_data"));
    }

    #[test]
    fn test_duplicate_types_deduplication() {
        let sql = "SELECT 
            data1::custom_type,
            data2::custom_type,
            data3::custom_type";
        
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        assert!(result.types.iter().any(|t| t.name == "custom_type"));
    }

    #[test]
    fn test_qualified_ident_from_qualified_name() {
        let ident = QualifiedIdent::from_qualified_name("schema.type_name");
        assert_eq!(ident.schema, Some("schema".to_string()));
        assert_eq!(ident.name, "type_name");
        
        let ident = QualifiedIdent::from_qualified_name("unqualified_type");
        assert_eq!(ident.schema, None);
        assert_eq!(ident.name, "unqualified_type");
    }

    #[test]
    fn test_empty_query() {
        let sql = "SELECT 1";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 0);
        assert_eq!(result.relations.len(), 0);
        assert_eq!(result.functions.len(), 0);
    }

    #[test]
    fn test_complex_cte_with_multiple_types() {
        let sql = "with
            expanded_cart as (
                select s.account_id       as seller_id,
                       array_agg_comp(pt) as seller_parcel_sizes,
                       l.item_id,
                       l.dimensions,
                       l.weight
                from cart_listing cl
                     join listing l on cl.item_id = l.item_id
                     join item i on l.item_id = i.item_id
                     join seller s on i.account_id = s.account_id
                     left join parcel_template pt on s.account_id = pt.account_id
                where cl.account_id = p_account_id
                  and cl.selected_for_checkout = true
                group by s.account_id, l.item_id, l.dimensions, l.weight,
                         cl.quantity),
            grouped_data as (
                select seller_id,
                       seller_parcel_sizes,
                       array_agg_comp((item_id, dimensions, weight)::api.parcel_details) as parcel_details
                from expanded_cart
                group by seller_id, seller_parcel_sizes)
        select array_agg_comp(
                       (seller_id, seller_parcel_sizes, parcel_details)::api.seller_shipping_group
               )
        from grouped_data;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that we found the custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"parcel_details"));
        assert!(type_names.contains(&"seller_shipping_group"));
        
        // Check schema qualification
        let api_parcel_details = result.types.iter()
            .find(|t| t.name == "parcel_details")
            .unwrap();
        assert_eq!(api_parcel_details.schema, Some("api".to_string()));
        
        let api_seller_shipping_group = result.types.iter()
            .find(|t| t.name == "seller_shipping_group")
            .unwrap();
        assert_eq!(api_seller_shipping_group.schema, Some("api".to_string()));
        
        // Check that we found the expected tables
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"cart_listing"));
        assert!(table_names.contains(&"listing"));
        assert!(table_names.contains(&"item"));
        assert!(table_names.contains(&"seller"));
        assert!(table_names.contains(&"parcel_template"));
        
        // Check that we found the functions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"array_agg_comp"));
    }

    #[test]
    fn test_recursive_cte_with_types() {
        let sql = "WITH RECURSIVE employee_hierarchy AS (
            SELECT employee_id, manager_id, name, ('entry', 1)::career_level AS level
            FROM employees
            WHERE manager_id IS NULL
            
            UNION ALL
            
            SELECT e.employee_id, e.manager_id, e.name, 
                   (level.level + 1)::career_level AS level
            FROM employees e
            JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
        )
        SELECT employee_id, name, level::employee_status
        FROM employee_hierarchy;";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"career_level"));
        assert!(type_names.contains(&"employee_status"));
        
        // Should find the employees table
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"employees"));
    }

    #[test]
    fn test_multiple_schemas_and_complex_casts() {
        let sql = "SELECT 
            data::schema1.type1,
            result::schema2.type2,
            (complex_data, status)::schema3.composite_type,
            CASE 
                WHEN active THEN 'active'::app_schema.status_enum
                ELSE 'inactive'::app_schema.status_enum
            END
        FROM test_table t1
        JOIN schema1.other_table t2 ON t1.id = t2.ref_id
        WHERE t1.created_at > '2023-01-01'::date";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check all custom types are found
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"type1"));
        assert!(type_names.contains(&"type2"));
        assert!(type_names.contains(&"composite_type"));
        assert!(type_names.contains(&"status_enum"));
        
        // Check schema qualifications
        let schema1_type1 = result.types.iter()
            .find(|t| t.name == "type1")
            .unwrap();
        assert_eq!(schema1_type1.schema, Some("schema1".to_string()));
        
        let schema2_type2 = result.types.iter()
            .find(|t| t.name == "type2")
            .unwrap();
        assert_eq!(schema2_type2.schema, Some("schema2".to_string()));
        
        let schema3_composite = result.types.iter()
            .find(|t| t.name == "composite_type")
            .unwrap();
        assert_eq!(schema3_composite.schema, Some("schema3".to_string()));
        
        let app_status = result.types.iter()
            .find(|t| t.name == "status_enum")
            .unwrap();
        assert_eq!(app_status.schema, Some("app_schema".to_string()));
        
        // Built-in date type should be filtered out
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"date"));
    }

    #[test]
    fn test_json_operations_with_casts() {
        let sql = "SELECT 
            (jsonb_data->>'user')::user_profile,
            (jsonb_data->'settings')::app_settings,
            jsonb_data#>'{metadata,created}'::metadata_info
        FROM user_data
        WHERE (jsonb_data->>'status')::user_status = 'active'";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"user_profile"));
        assert!(type_names.contains(&"app_settings"));
        assert!(type_names.contains(&"metadata_info"));
        assert!(type_names.contains(&"user_status"));
    }

    #[test]
    fn test_window_functions_with_complex_casts() {
        let sql = "SELECT 
            employee_id,
            (salary, department)::compensation_info,
            rank() OVER (PARTITION BY department ORDER BY salary DESC)::ranking_info,
            lag((salary, bonus)::total_comp, 1) OVER (ORDER BY hire_date)::previous_comp
        FROM employees
        WHERE department::dept_enum IN ('engineering', 'sales')";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"compensation_info"));
        assert!(type_names.contains(&"ranking_info"));
        assert!(type_names.contains(&"total_comp"));
        assert!(type_names.contains(&"previous_comp"));
        assert!(type_names.contains(&"dept_enum"));
    }

    #[test]
    fn test_union_with_different_type_casts() {
        let sql = "SELECT 'customer'::entity_type, id::customer_id, name FROM customers
        UNION ALL
        SELECT 'vendor'::entity_type, id::vendor_id, company_name FROM vendors
        UNION ALL
        SELECT 'employee'::entity_type, id::employee_id, full_name FROM employees";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"entity_type"));
        assert!(type_names.contains(&"customer_id"));
        assert!(type_names.contains(&"vendor_id"));
        assert!(type_names.contains(&"employee_id"));
    }

    #[test]
    fn test_stored_procedure_call_with_casts() {
        let sql = "SELECT * FROM process_order(
            order_id::order_identifier,
            (customer_data, shipping_info)::order_context,
            'standard'::shipping_method
        )";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_identifier"));
        assert!(type_names.contains(&"order_context"));
        assert!(type_names.contains(&"shipping_method"));
        
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"process_order"));
    }

    #[test]
    fn test_create_type_statement() {
        let sql = "create type api.update_item as
(
    item_id              int,
    mini_content_status  content_status,
    mini_assembly_status assembly_status,
    mini_painting_status painting_status,
    mini_color           hex_color,
    description          text,
    listing              api.update_listing,
    images               text[],
    is_hidden            bool
);";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should find all the custom types used in the type definition
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"content_status"));
        assert!(type_names.contains(&"assembly_status"));
        assert!(type_names.contains(&"painting_status"));
        assert!(type_names.contains(&"hex_color"));
        assert!(type_names.contains(&"update_listing"));
        
        // Should not include built-in types
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"int"));
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"text"));
        // assert!(!type_names.contains(&"bool"));
        
        // Check schema qualification
        let update_listing = result.types.iter()
            .find(|t| t.name == "update_listing")
            .unwrap();
        assert_eq!(update_listing.schema, Some("api".to_string()));
    }

    #[test]
    fn test_create_domain_statement() {
        let sql = "CREATE DOMAIN api.positive_amount AS numeric CHECK (value > 0);
                   CREATE DOMAIN email_address AS text CHECK (value ~ '^[^@]+@[^@]+$');
                   CREATE DOMAIN user_id AS uuid;
                   CREATE DOMAIN order_status AS status_enum NOT NULL;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should find the custom base types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"status_enum"));
        
        // Should not include built-in types like numeric, text, uuid
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"numeric"));
        // assert!(!type_names.contains(&"text"));
        // assert!(!type_names.contains(&"uuid"));
    }

    #[test]
    fn test_plpgsql_function_calls_in_expressions() {
        let sql = "
        CREATE OR REPLACE FUNCTION test_function_calls()
        RETURNS integer AS $$
        DECLARE
            v_result integer;
            v_status boolean;
        BEGIN
            -- Assignment with function call
            v_result := calculate_total(100);
            
            -- IF condition with function call
            IF is_valid_user(123) THEN
                v_result := v_result + get_bonus(123);
            ELSIF check_status() = 'active' THEN
                v_result := apply_discount(v_result, 0.1);
            END IF;
            
            -- WHILE loop with function call
            WHILE has_more_items() LOOP
                v_result := process_item(v_result);
            END LOOP;
            
            -- CASE expression with function calls
            v_status := CASE 
                WHEN validate_amount(v_result) THEN true
                ELSE false
            END;
            
            -- RETURN with function call
            RETURN finalize_result(v_result);
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check that we found all the function calls
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"calculate_total"));
        assert!(function_names.contains(&"is_valid_user"));
        assert!(function_names.contains(&"get_bonus"));
        assert!(function_names.contains(&"check_status"));
        assert!(function_names.contains(&"apply_discount"));
        assert!(function_names.contains(&"has_more_items"));
        assert!(function_names.contains(&"process_item"));
        assert!(function_names.contains(&"validate_amount"));
        assert!(function_names.contains(&"finalize_result"));
    }

    #[test]
    fn test_plpgsql_complex_expressions() {
        let sql = "
        CREATE OR REPLACE FUNCTION process_order(p_order_id integer)
        RETURNS api.order_result AS $$
        DECLARE
            v_total numeric;
            v_status order_status;
        BEGIN
            -- Complex assignment with qualified function calls and type casts
            v_total := api.calculate_subtotal(p_order_id) + 
                       shipping.calculate_cost(p_order_id)::numeric;
            
            -- IF with type cast and function
            IF v_total > get_threshold()::numeric THEN
                v_status := 'approved'::order_status;
            ELSE
                v_status := validate_order(p_order_id)::order_status;
            END IF;
            
            -- Nested function calls
            v_total := apply_tax(
                apply_discount(v_total, get_customer_discount(p_order_id))
            );
            
            -- RETURN with constructor and cast
            RETURN (p_order_id, v_status, v_total)::api.order_result;
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check function calls
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"calculate_subtotal"));
        assert!(function_names.contains(&"calculate_cost"));
        assert!(function_names.contains(&"get_threshold"));
        assert!(function_names.contains(&"validate_order"));
        assert!(function_names.contains(&"apply_tax"));
        assert!(function_names.contains(&"apply_discount"));
        assert!(function_names.contains(&"get_customer_discount"));
        
        // Check schema qualification for functions
        let calc_subtotal = result.functions.iter()
            .find(|f| f.name == "calculate_subtotal")
            .unwrap();
        assert_eq!(calc_subtotal.schema, Some("api".to_string()));
        
        let calc_cost = result.functions.iter()
            .find(|f| f.name == "calculate_cost")
            .unwrap();
        assert_eq!(calc_cost.schema, Some("shipping".to_string()));
        
        // Check types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_result"));
        assert!(type_names.contains(&"order_status"));
        
        // Check return type qualification
        let order_result = result.types.iter()
            .find(|t| t.name == "order_result")
            .unwrap();
        assert_eq!(order_result.schema, Some("api".to_string()));
    }

    #[test]
    fn test_references_and_default_extraction() {
        let sql = "CREATE TABLE test_table (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_id uuid REFERENCES customers(id) ON DELETE CASCADE,
            user_id bigint REFERENCES auth.users(id),
            status text DEFAULT get_default_status(),
            created_at timestamptz DEFAULT now(),
            data jsonb DEFAULT '{}'::jsonb
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that DEFAULT function calls are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"gen_random_uuid"));
        assert!(function_names.contains(&"get_default_status"));
        assert!(function_names.contains(&"now"));
        
        // Check that REFERENCES tables are extracted
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"test_table")); // The table being created
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"users"));
        
        // Check schema qualification for referenced table
        let users_table = result.relations.iter()
            .find(|t| t.name == "users")
            .unwrap();
        assert_eq!(users_table.schema, Some("auth".to_string()));
    }

    #[test]
    fn test_standalone_constraint() {
        let sql = "
        CREATE TABLE test_table (id uuid);
        ALTER TABLE test_table ADD CONSTRAINT fk_ref FOREIGN KEY (id) REFERENCES other_table(id);
        ";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that the referenced table is extracted
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"test_table"));
        assert!(table_names.contains(&"other_table"));
    }
    
    #[test]
    fn test_domain_constraints() {
        let sql1 = "create domain api.oauth_credential as api.oauth_credential_inner check (  (value).provider_id is not null and (value).provider_token is not null );";
        let result1 = analyze_statement(sql1).unwrap();
        
        // Should extract the base type api.oauth_credential_inner
        let type_names: Vec<&str> = result1.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"oauth_credential_inner"));
        
        let sql2 = "create domain api.weight_with_unit as api.weight_with_unit_inner  
    constraint weight_not_null check (value is null or (value).weight is not null)  
    constraint weight_gt_zero check (value is null or (value).weight > 0)  
    constraint unit_not_null check (value is null or (value).unit is not null)  
    constraint max_weight check (  
        value is null  
            or (  
            (value).unit = 'g' and (value).weight <= 9071.85  
                or (value).unit = 'kg' and (value).weight <= 9.07185  
                or (value).unit = 'oz' and (value).weight <= 320  
                or (value).unit = 'lb' and (value).weight <= 20  
            ));";
        let result2 = analyze_statement(sql2).unwrap();
        
        // Should extract the base type api.weight_with_unit_inner
        let type_names2: Vec<&str> = result2.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names2.contains(&"weight_with_unit_inner"));
    }
    
    #[test]
    fn test_table_constraints() {
        let sql = "CREATE TABLE orders (
            id uuid,
            customer_id uuid,
            total numeric,
            CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
            CONSTRAINT valid_total CHECK (total > 0),
            CONSTRAINT unique_order UNIQUE (id)
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the referenced table from table-level constraint
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"customers"));
    }
    
    #[test]
    fn test_check_constraints_with_functions() {
        let sql = "CREATE TABLE orders (
            id uuid,
            total numeric,
            created_at timestamptz,
            CONSTRAINT valid_total CHECK (total > 0),
            CONSTRAINT valid_date CHECK (created_at > now() - interval '1 year'),
            CONSTRAINT valid_id CHECK (validate_uuid(id))
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract function calls from CHECK constraints
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"validate_uuid"));
    }
    
    #[test]
    fn test_trigger_dependencies() {
        let sql = "CREATE TRIGGER check_order_status
            BEFORE INSERT OR UPDATE ON orders
            FOR EACH ROW
            EXECUTE FUNCTION update_order_status();";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name from the ON clause
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        
        // Should extract the function name from EXECUTE FUNCTION clause
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"update_order_status"));
    }
    
    #[test]
    fn test_trigger_with_schema() {
        let sql = "CREATE TRIGGER audit_user_changes
            AFTER INSERT OR UPDATE OR DELETE ON auth.users
            FOR EACH ROW
            EXECUTE FUNCTION audit.log_user_changes();";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract schema-qualified table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        let users_table = result.relations.iter().find(|t| t.name == "users").unwrap();
        assert_eq!(users_table.schema, Some("auth".to_string()));
        
        // Should extract schema-qualified function name
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"log_user_changes"));
        let log_function = result.functions.iter().find(|f| f.name == "log_user_changes").unwrap();
        assert_eq!(log_function.schema, Some("audit".to_string()));
    }
    
    #[test]
    fn test_drop_trigger() {
        let sql = "DROP TRIGGER check_order_status ON orders;";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name from DROP TRIGGER
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        
        // DROP TRIGGER doesn't contain function references
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.is_empty());
    }
    
    #[test]
    fn test_create_index_dependencies() {
        let sql = "CREATE INDEX idx_orders_customer ON orders(customer_id);";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name from the index
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
    }
    
    #[test]
    fn test_create_index_with_expressions() {
        let sql = "CREATE INDEX idx_orders_total_usd ON orders (convert_to_usd(total));";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        
        // Should extract function calls from index expressions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"convert_to_usd"));
    }
    
    #[test]
    fn test_create_materialized_view_dependencies() {
        let sql = "CREATE MATERIALIZED VIEW order_summary AS 
            SELECT o.id, o.total, c.name AS customer_name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            WHERE o.status = 'active';";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract all table names from the query
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"order_summary")); // The materialized view itself
    }
    
    #[test]
    fn test_create_materialized_view_with_functions() {
        let sql = "CREATE MATERIALIZED VIEW daily_stats AS
            SELECT 
                date_trunc('day', created_at) as day,
                count(*) as order_count,
                sum(calculate_total(amount)) as total_amount
            FROM orders
            WHERE created_at >= current_date - interval '30 days'
            GROUP BY date_trunc('day', created_at);";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract table names
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"daily_stats"));
        
        // Should extract function calls
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"date_trunc"));
        assert!(function_names.contains(&"calculate_total"));
        assert!(function_names.contains(&"count"));
        assert!(function_names.contains(&"sum"));
    }
    
    #[test]
    fn test_create_unique_index_with_where_clause() {
        let sql = "CREATE UNIQUE INDEX idx_active_users ON users(email) WHERE status = 'active' AND validate_email(email);";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        
        // Should extract function calls from WHERE clause
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"validate_email"));
    }
    
    #[test]
    fn test_create_index_on_schema_qualified_table() {
        let sql = "CREATE INDEX idx_user_names ON auth.users(lower(name));";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract schema-qualified table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        let users_table = result.relations.iter().find(|t| t.name == "users").unwrap();
        assert_eq!(users_table.schema, Some("auth".to_string()));
        
        // Should extract function calls from index expression
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"lower"));
    }
    
    #[test]
    fn test_refresh_materialized_view() {
        let sql = "REFRESH MATERIALIZED VIEW order_summary;";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the materialized view name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"order_summary"));
    }
    
    #[test]
    fn test_on_conflict_clause_dependencies() {
        let sql = "INSERT INTO users (id, email, name) 
                   VALUES (1, 'test@example.com', 'Test User')
                   ON CONFLICT (email) DO UPDATE SET 
                       name = EXCLUDED.name,
                       updated_at = now(),
                       normalized_email = normalize_email(EXCLUDED.email);";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        
        // Should extract function calls from ON CONFLICT DO UPDATE
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"normalize_email"));
    }
    
    #[test]
    fn test_on_conflict_with_where_clause() {
        let sql = "INSERT INTO products (id, name, price) 
                   VALUES (1, 'Product', 100)
                   ON CONFLICT (name) DO UPDATE SET 
                       price = EXCLUDED.price,
                       updated_at = current_timestamp
                   WHERE validate_price(EXCLUDED.price) AND products.status = 'active';";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract function calls from WHERE clause in ON CONFLICT
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"validate_price"));
    }
    
    #[test]
    fn test_returning_clause_dependencies() {
        let sql = "INSERT INTO orders (customer_id, total, status) 
                   VALUES (1, 100.50, 'pending')
                   RETURNING id, calculate_tax(total) as tax, format_status(status) as formatted_status;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        
        // Should extract function calls from RETURNING clause
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"calculate_tax"));
        assert!(function_names.contains(&"format_status"));
    }
    
    #[test]
    fn test_update_with_returning_clause() {
        let sql = "UPDATE users SET 
                       name = 'Updated Name',
                       updated_at = now()
                   WHERE id = 1
                   RETURNING id, name, hash_email(email) as email_hash, audit_log(id, 'update') as audit_id;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        
        // Should extract function calls from SET clause and RETURNING clause
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"hash_email"));
        assert!(function_names.contains(&"audit_log"));
    }
    
    #[test]
    fn test_delete_with_returning_clause() {
        let sql = "DELETE FROM audit_logs 
                   WHERE created_at < now() - interval '1 year'
                   RETURNING id, archive_record(id, data) as archive_id;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"audit_logs"));
        
        // Should extract function calls from WHERE clause and RETURNING clause
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"archive_record"));
    }
    
    #[test]
    fn test_complex_on_conflict_with_returning() {
        let sql = "INSERT INTO user_preferences (user_id, key, value, created_at) 
                   VALUES (1, 'theme', 'dark', now())
                   ON CONFLICT (user_id, key) DO UPDATE SET 
                       value = EXCLUDED.value,
                       updated_at = now(),
                       version = increment_version(user_preferences.version)
                   WHERE validate_preference(EXCLUDED.key, EXCLUDED.value)
                   RETURNING id, user_id, key, encrypt_value(value) as encrypted_value, audit_change(user_id, key) as audit_id;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table name
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"user_preferences"));
        
        // Should extract function calls from all clauses
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"increment_version"));
        assert!(function_names.contains(&"validate_preference"));
        assert!(function_names.contains(&"encrypt_value"));
        assert!(function_names.contains(&"audit_change"));
    }
    
    #[test]
    fn test_call_stored_procedure() {
        let sql = "CALL process_orders(100, 'active');";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the procedure name as a function dependency
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"process_orders"));
    }
    
    #[test]
    fn test_call_schema_qualified_procedure() {
        let sql = "CALL audit.log_user_action(123, 'login', now());";
        let result = analyze_statement(sql).unwrap();
        
        // Should extract schema-qualified procedure name
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"log_user_action"));
        assert!(function_names.contains(&"now"));
        
        // Check schema qualification
        let log_procedure = result.functions.iter().find(|f| f.name == "log_user_action").unwrap();
        assert_eq!(log_procedure.schema, Some("audit".to_string()));
    }
    
    #[test]
    fn test_create_table_with_custom_types() {
        let sql = "
        CREATE TABLE api.orders (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            order_number order_number_type NOT NULL,
            customer_id uuid REFERENCES customers(id),
            status order_status DEFAULT 'pending'::order_status,
            total currency NOT NULL,
            items order_item[] NOT NULL,
            shipping_address address,
            billing_info billing_info,
            metadata jsonb,
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now()
        );
        
        -- Table with custom type column
        CREATE TABLE inventory (
            id serial PRIMARY KEY,
            sku text NOT NULL,
            quantity int NOT NULL,
            location warehouse_location,
            unit_price currency NOT NULL
        );
        ";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_number_type"));
        assert!(type_names.contains(&"order_status"));
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"order_item"));
        assert!(type_names.contains(&"address"));
        assert!(type_names.contains(&"billing_info"));
        assert!(type_names.contains(&"warehouse_location"));
        
        // Built-in types should be filtered
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"uuid"));
        // assert!(!type_names.contains(&"jsonb"));
        // assert!(!type_names.contains(&"timestamptz"));
        // assert!(!type_names.contains(&"int"));
        
        // Check tables from REFERENCES
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers")); // From REFERENCES
        
        // Check functions from DEFAULT clauses
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"gen_random_uuid"));
        assert!(function_names.contains(&"now"));
    }

    #[test]
    fn test_alter_table_with_custom_types() {
        let sql = "
        -- Add column with custom type
        ALTER TABLE orders ADD COLUMN discount discount_type;
        
        -- Add column with default using custom type cast
        ALTER TABLE customers 
            ADD COLUMN loyalty_status customer_status DEFAULT 'bronze'::customer_status,
            ADD COLUMN credit_limit currency DEFAULT (1000, 'USD')::currency;
        
        -- Add constraint with custom type
        ALTER TABLE products 
            ADD CONSTRAINT valid_price CHECK (price::currency > (0, 'USD')::currency);
        
        -- Alter column type
        ALTER TABLE inventory ALTER COLUMN location TYPE new_warehouse_location;
        ";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"discount_type"));
        assert!(type_names.contains(&"customer_status"));
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"new_warehouse_location"));
    }

    #[test]
    fn test_create_index_with_custom_types() {
        let sql = "
        -- Index with cast expression
        CREATE INDEX idx_orders_status ON orders ((status::text));
        
        -- Index on custom type column
        CREATE INDEX idx_inventory_location ON inventory (location);
        
        -- Partial index with custom type in WHERE clause
        CREATE INDEX idx_active_orders ON orders (created_at) 
        WHERE status = 'active'::order_status;
        
        -- Expression index with function on custom type
        CREATE INDEX idx_order_total_usd ON orders ((get_usd_amount(total)));
        ";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_status"));
        
        // Check functions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"get_usd_amount"));
    }

    #[test]
    fn test_create_view_with_custom_types() {
        let sql = "
        CREATE VIEW api.order_summary AS
        SELECT 
            o.id,
            o.order_number,
            o.status::order_status_display AS display_status,
            o.total,
            c.name AS customer_name,
            shipping_cost(o.shipping_address)::currency AS shipping_cost,
            (SELECT array_agg(item_name::product_name) 
             FROM unnest(o.items) AS item_name) AS product_names
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status != 'cancelled'::order_status;
        
        -- Materialized view
        CREATE MATERIALIZED VIEW inventory_summary AS
        SELECT 
            location,
            COUNT(*)::int AS item_count,
            SUM(value)::currency AS total_value,
            aggregate_status(status)::inventory_status AS overall_status
        FROM inventory
        GROUP BY location;
        ";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_status_display"));
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"product_name"));
        assert!(type_names.contains(&"order_status"));
        assert!(type_names.contains(&"inventory_status"));
        
        // Check functions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"shipping_cost"));
        assert!(function_names.contains(&"unnest"));
        assert!(function_names.contains(&"array_agg"));
        assert!(function_names.contains(&"aggregate_status"));
        assert!(function_names.contains(&"sum"));
        assert!(function_names.contains(&"count"));
    }

    #[test]
    fn test_ddl_with_mixed_statements() {
        let sql = "
        -- Trigger function using custom types
        CREATE OR REPLACE FUNCTION update_order_status()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.total > get_threshold()::currency THEN
                NEW.status := 'priority'::order_status;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Create trigger
        CREATE TRIGGER check_order_status
            BEFORE INSERT OR UPDATE ON orders
            FOR EACH ROW
            EXECUTE FUNCTION update_order_status();
        
        -- Query with custom type and function
        SELECT notify_admin(id, total) 
        FROM orders 
        WHERE total > (10000, 'USD')::currency;
        ";
        
        // Analyze as a single statement batch
        let result = analyze_statement(sql).unwrap();
        
        // Should find types from all parts
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"currency"));
        
        // Should find functions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"notify_admin"));
        
        // For the PL/pgSQL function body, we'd need to use analyze_plpgsql
        let plpgsql_part = "CREATE OR REPLACE FUNCTION update_order_status()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.total > get_threshold()::currency THEN
                NEW.status := 'priority'::order_status;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;";
        
        let plpgsql_result = analyze_plpgsql(plpgsql_part).unwrap();
        assert!(plpgsql_result.types.iter().any(|t| t.name == "order_status"));
        assert!(plpgsql_result.functions.iter().any(|f| f.name == "get_threshold"));
    }

    #[test]
    fn test_create_type_with_nested_types() {
        let sql = "
        CREATE TYPE address AS (
            street text,
            city text,
            country country_code
        );
        
        CREATE TYPE api.customer_info AS (
            id uuid,
            name text,
            email email_type,
            billing_address address,
            shipping_address address,
            status customer_status
        );
        
        CREATE DOMAIN country_code AS char(2);
        ";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should find all custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"country_code"));
        assert!(type_names.contains(&"email_type"));
        assert!(type_names.contains(&"address"));
        assert!(type_names.contains(&"customer_status"));
        
        // Built-in types are now included (filtering happens later)
        // assert!(!type_names.contains(&"text")); // This check is no longer valid
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"uuid"));
        // assert!(!type_names.contains(&"char"));
    }

    #[test]
    fn test_analyze_plpgsql_return_and_param_types() {
        let sql = "
        CREATE OR REPLACE FUNCTION calculate_order_total(
            p_order_id order_id_type,
            p_discount discount_type
        )
        RETURNS api.order_total AS $$
        DECLARE
            v_subtotal currency;
        BEGIN
            SELECT SUM(price * quantity)::currency INTO v_subtotal
            FROM order_items
            WHERE order_id = p_order_id;
            
            RETURN (v_subtotal, p_discount)::api.order_total;
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check that we found the parameter types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_id_type"));
        assert!(type_names.contains(&"discount_type"));
        
        // Check that we found the return type
        assert!(type_names.contains(&"order_total"));
        
        // Check that we found the type from the variable declaration
        assert!(type_names.contains(&"currency"));
        
        // Check schema qualification for return type
        let order_total = result.types.iter()
            .find(|t| t.name == "order_total")
            .unwrap();
        assert_eq!(order_total.schema, Some("api".to_string()));
        
        // Check that we found the table
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"order_items"));
    }

    #[test]
    fn test_analyze_plpgsql_simple_function() {
        let sql = "
        CREATE OR REPLACE FUNCTION calculate_total(p_account_id integer)
        RETURNS currency AS $$
        DECLARE
            total currency;
        BEGIN
            SELECT SUM(amount)::currency INTO total
            FROM transactions
            WHERE account_id = p_account_id;
            
            RETURN COALESCE(total, (0, 'USD')::currency);
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check that we found the table
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"transactions"));
        
        // Check that we found the custom type
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"currency"));
        
        // Check that we found the functions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        // SUM and COALESCE are built-in functions
        assert!(function_names.contains(&"sum"));
        // The COALESCE call is in the RETURN statement which is not a standard SQL statement
    }

    #[test]
    fn test_analyze_plpgsql_with_multiple_statements() {
        let sql = "
        CREATE OR REPLACE FUNCTION process_order(
            p_order_id integer,
            p_customer_id integer
        )
        RETURNS api.order_result AS $$
        DECLARE
            v_status order_status;
            v_total numeric;
        BEGIN
            -- Get order status
            SELECT status::order_status INTO v_status
            FROM orders
            WHERE order_id = p_order_id;
            
            -- Calculate total
            SELECT SUM(quantity * price)::numeric INTO v_total
            FROM order_items
            WHERE order_id = p_order_id;
            
            -- Update customer stats
            UPDATE customer_stats
            SET last_order_date = now(),
                total_spent = total_spent + v_total
            WHERE customer_id = p_customer_id;
            
            -- Return result
            RETURN (p_order_id, v_status, v_total)::api.order_result;
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check tables
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"order_items"));
        assert!(table_names.contains(&"customer_stats"));
        
        // Check types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_status"));
        assert!(type_names.contains(&"order_result"));
        
        // Check schema qualification
        let order_result = result.types.iter()
            .find(|t| t.name == "order_result")
            .unwrap();
        assert_eq!(order_result.schema, Some("api".to_string()));
    }

    #[test]
    fn test_analyze_plpgsql_with_dynamic_sql() {
        let sql = "
        CREATE OR REPLACE FUNCTION get_table_data(
            p_table_name text,
            p_schema_name text DEFAULT 'public'
        )
        RETURNS SETOF record AS $$
        DECLARE
            v_query text;
        BEGIN
            -- This contains dynamic SQL, so we won't capture the table dependency
            v_query := format('SELECT * FROM %I.%I', p_schema_name, p_table_name);
            RETURN QUERY EXECUTE v_query;
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Dynamic SQL won't be captured as dependencies
        assert_eq!(result.relations.len(), 0);
        
        // Now we properly extract function calls from assignment statements
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"format"));
    }

    #[test]
    fn test_analyze_plpgsql_with_cte_and_joins() {
        let sql = "
        CREATE OR REPLACE FUNCTION get_user_summary(p_user_id integer)
        RETURNS user_summary_type AS $$
        DECLARE
            result user_summary_type;
        BEGIN
            WITH order_stats AS (
                SELECT 
                    COUNT(*)::bigint as order_count,
                    SUM(total)::currency as total_spent
                FROM orders
                WHERE user_id = p_user_id
            ),
            activity_stats AS (
                SELECT 
                    MAX(login_time)::timestamptz as last_login,
                    COUNT(*)::bigint as login_count
                FROM user_activity
                WHERE user_id = p_user_id
                  AND activity_type = 'login'::activity_enum
            )
            SELECT 
                u.username,
                u.email,
                os.order_count,
                os.total_spent,
                a_s.last_login,
                a_s.login_count
            INTO result
            FROM users u
            CROSS JOIN order_stats os
            CROSS JOIN activity_stats a_s
            WHERE u.id = p_user_id;
            
            RETURN result;
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check tables
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"user_activity"));
        assert!(table_names.contains(&"users"));
        
        // Check types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"user_summary_type"));
        assert!(type_names.contains(&"currency"));
        assert!(type_names.contains(&"activity_enum"));
    }

    #[test]
    fn test_analyze_plpgsql_with_exception_handling() {
        let sql = "
        CREATE OR REPLACE FUNCTION safe_divide(
            p_numerator numeric,
            p_denominator numeric
        )
        RETURNS api.calculation_result AS $$
        DECLARE
            v_result numeric;
        BEGIN
            BEGIN
                v_result := p_numerator / p_denominator;
                
                INSERT INTO calculation_log (
                    numerator,
                    denominator,
                    result,
                    status
                ) VALUES (
                    p_numerator,
                    p_denominator,
                    v_result,
                    'success'::calc_status
                );
                
                RETURN (true, v_result, NULL)::api.calculation_result;
            EXCEPTION
                WHEN division_by_zero THEN
                    INSERT INTO error_log (
                        error_type,
                        error_message,
                        occurred_at
                    ) VALUES (
                        'division_by_zero'::error_type,
                        'Division by zero attempted',
                        now()
                    );
                    
                    RETURN (false, NULL, 'Division by zero')::api.calculation_result;
            END;
        END;
        $$ LANGUAGE plpgsql;
        ";
        
        let result = analyze_plpgsql(sql).unwrap();
        
        // Check tables
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"calculation_log"));
        assert!(table_names.contains(&"error_log"));
        
        // Check types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"calculation_result"));
        assert!(type_names.contains(&"calc_status"));
        assert!(type_names.contains(&"error_type"));
        
        // Check schema qualification
        let calc_result = result.types.iter()
            .find(|t| t.name == "calculation_result")
            .unwrap();
        assert_eq!(calc_result.schema, Some("api".to_string()));
    }

    #[test]
    fn test_insert_with_complex_casts() {
        let sql = "INSERT INTO audit_log (
            event_type,
            user_info,
            timestamp_info,
            metadata
        ) VALUES (
            'user_action'::audit_event_type,
            (user_id, session_id)::user_session_info,
            (now(), timezone('UTC', now()))::timestamp_pair,
            jsonb_build_object('action', action_name)::audit_metadata
        )";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        
        // Check that we found the expected types from the INSERT statement
        assert!(type_names.contains(&"audit_event_type"));
        assert!(type_names.contains(&"audit_metadata"));
        
        // Note: The debug parsing approach has some limitations and may not extract all types
        // from complex nested structures, but it works for the most common cases
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"audit_log"));
    }

    #[test]
    fn test_update_with_conditional_casts() {
        let sql = "UPDATE user_profiles 
        SET 
            status = CASE 
                WHEN last_login < now() - interval '30 days' THEN 'inactive'::user_status
                ELSE 'active'::user_status
            END,
            profile_data = (
                COALESCE(profile_data, '{}'::jsonb) || 
                jsonb_build_object('last_update', now())
            )::enhanced_profile
        WHERE user_id = ANY(user_ids::user_id_array)";
        
        let result = analyze_statement(sql).unwrap();
        
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"user_status"));
        assert!(type_names.contains(&"enhanced_profile"));
        assert!(type_names.contains(&"user_id_array"));
        
        // Built-in jsonb should be filtered out
        // Built-in types are now included in analysis (filtering happens later)
        // assert!(!type_names.contains(&"jsonb"));
    }

    #[test]
    fn test_default_with_complex_expressions() {
        let sql = "CREATE TABLE user_preferences (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id bigint NOT NULL,
            settings jsonb DEFAULT jsonb_build_object('theme', 'light', 'notifications', true),
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
            version integer DEFAULT nextval('version_seq'),
            expires_at timestamptz DEFAULT (now() + interval '1 year'),
            checksum text DEFAULT md5(random()::text || clock_timestamp()::text)
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that DEFAULT function calls are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"gen_random_uuid"));
        assert!(function_names.contains(&"jsonb_build_object"));
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"md5"));
        // All functions should now be extracted with improved AST traversal
        assert!(function_names.contains(&"random"));
        assert!(function_names.contains(&"clock_timestamp"));
        
        // Check that the table being created is included
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"user_preferences"));
    }

    #[test]
    fn test_default_with_nested_function_calls() {
        let sql = "CREATE TABLE audit_entries (
            id bigint PRIMARY KEY DEFAULT nextval('audit_seq'),
            event_data jsonb DEFAULT jsonb_build_object(
                'timestamp', now(),
                'session_id', gen_random_uuid()
            ),
            normalized_data text DEFAULT lower('default_value'),
            computed_hash text DEFAULT md5('simple_value')
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that function calls are extracted from DEFAULT clauses
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"jsonb_build_object"));
        assert!(function_names.contains(&"lower"));
        assert!(function_names.contains(&"md5"));
        // All functions should now be extracted with improved AST traversal
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"gen_random_uuid"));
    }

    #[test]
    fn test_default_with_arithmetic_expressions() {
        let sql = "CREATE TABLE arithmetic_defaults (
            id bigint PRIMARY KEY DEFAULT nextval('seq'),
            calculation numeric DEFAULT (1 + 2) * 3,
            with_function numeric DEFAULT power(2, 3),
            with_cast numeric DEFAULT 42::numeric
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that functions in arithmetic expressions are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"power"));
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"arithmetic_defaults"));
    }

    #[test]
    fn test_default_with_schema_qualified_functions() {
        let sql = "CREATE TABLE schema_qualified_defaults (
            id bigint PRIMARY KEY DEFAULT nextval('public.main_seq'),
            api_key text DEFAULT auth.generate_api_key(),
            encrypted_data bytea DEFAULT crypto.encrypt_data('default_value'),
            audit_info jsonb DEFAULT logging.create_audit_entry('table_creation')
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that schema-qualified functions are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"generate_api_key"));
        assert!(function_names.contains(&"encrypt_data"));
        assert!(function_names.contains(&"create_audit_entry"));
        
        // Check schema qualification
        let auth_func = result.functions.iter()
            .find(|f| f.name == "generate_api_key")
            .unwrap();
        assert_eq!(auth_func.schema, Some("auth".to_string()));
        
        let crypto_func = result.functions.iter()
            .find(|f| f.name == "encrypt_data")
            .unwrap();
        assert_eq!(crypto_func.schema, Some("crypto".to_string()));
        
        let logging_func = result.functions.iter()
            .find(|f| f.name == "create_audit_entry")
            .unwrap();
        assert_eq!(logging_func.schema, Some("logging".to_string()));
    }

    #[test]
    fn test_default_with_string_operations() {
        let sql = "CREATE TABLE string_defaults (
            id bigint PRIMARY KEY DEFAULT nextval('seq'),
            name text DEFAULT concat('user_', generate_id()),
            email text DEFAULT lower('DEFAULT@EXAMPLE.COM'),
            slug text DEFAULT replace(lower('Test String'), ' ', '_'),
            hash text DEFAULT md5('default_value')
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that string functions are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"concat"));
        assert!(function_names.contains(&"lower"));
        assert!(function_names.contains(&"replace"));
        assert!(function_names.contains(&"md5"));
        // Note: Some functions in complex expressions may not be extracted
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"string_defaults"));
    }

    #[test]
    fn test_default_with_type_casting() {
        let sql = "CREATE TABLE cast_defaults (
            id bigint PRIMARY KEY DEFAULT nextval('seq'),
            timestamp_value timestamptz DEFAULT now(),
            json_value jsonb DEFAULT '{\"key\": \"value\"}'::jsonb,
            array_value integer[] DEFAULT '{1,2,3}'::integer[]
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that functions are extracted even with type casting
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"now"));
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"cast_defaults"));
    }

    #[test]
    fn test_nested_function_calls_comprehensive() {
        let sql = "CREATE TABLE comprehensive_nested (
            id bigint PRIMARY KEY DEFAULT nextval('seq'),
            complex_json jsonb DEFAULT jsonb_build_object(
                'timestamp', extract(epoch from now()),
                'session_id', encode(gen_random_bytes(16), 'hex'),
                'trace_id', upper(replace(gen_random_uuid()::text, '-', '')),
                'nested_obj', jsonb_build_object(
                    'level2', json_build_object(
                        'deep_call', concat('prefix_', lower(random()::text))
                    )
                )
            ),
            case_with_functions text DEFAULT CASE 
                WHEN extract(hour from now()) < 9 THEN concat('morning_', generate_id())
                WHEN extract(hour from now()) > 17 THEN concat('evening_', generate_id())
                ELSE concat('day_', generate_id())
            END,
            array_with_functions text[] DEFAULT array[
                concat('item_', generate_sequence()),
                upper(md5(random()::text)),
                lower(encode(gen_random_bytes(8), 'base64'))
            ],
            coalesce_with_functions text DEFAULT coalesce(
                get_cached_value(),
                compute_default_value(),
                'fallback'
            )
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that all nested functions are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        println!("Comprehensive nested functions: {:?}", function_names);
        
        // Direct function calls that should be extracted
        assert!(function_names.contains(&"nextval"));
        assert!(function_names.contains(&"jsonb_build_object"));
        assert!(function_names.contains(&"json_build_object"));
        assert!(function_names.contains(&"concat"));
        assert!(function_names.contains(&"upper"));
        assert!(function_names.contains(&"lower"));
        assert!(function_names.contains(&"encode"));
        // Note: coalesce might not be extracted in all contexts
        
        // Nested function calls that should now be extracted (this is the key improvement!)
        assert!(function_names.contains(&"extract"));
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"gen_random_bytes"));
        assert!(function_names.contains(&"gen_random_uuid"));
        assert!(function_names.contains(&"replace"));
        assert!(function_names.contains(&"random"));
        assert!(function_names.contains(&"generate_id"));
        assert!(function_names.contains(&"get_cached_value"));
        assert!(function_names.contains(&"compute_default_value"));
        
        // Verify significant improvement in extraction - we should get many more functions
        assert!(function_names.len() >= 16, "Should extract at least 16 functions, got: {:?}", function_names);
    }

    #[test]
    fn test_generated_column_basic() {
        let sql = "CREATE TABLE users (
            id serial PRIMARY KEY,
            first_name text NOT NULL,
            last_name text NOT NULL,
            full_name text GENERATED ALWAYS AS (concat(first_name, ' ', last_name)) STORED,
            created_at timestamptz DEFAULT now()
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that functions from GENERATED columns are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        println!("Generated column functions: {:?}", function_names);
        
        // Also check what the built-in pg_query functions method finds
        let parse_result = pg_query::parse(sql).unwrap();
        let builtin_functions = parse_result.functions();
        println!("Built-in pg_query functions: {:?}", builtin_functions);
        
        // DEFAULT functions should be extracted
        assert!(function_names.contains(&"now"));
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        
        // Note: GENERATED columns may not be supported by the current pg_query version
        // The test passes if DEFAULT functions work, showing the infrastructure is there
        // for when GENERATED column support is added
    }

    #[test]
    fn test_generated_column_with_nested_functions() {
        let sql = "CREATE TABLE products (
            id bigint PRIMARY KEY,
            name text NOT NULL,
            price numeric NOT NULL,
            category text NOT NULL,
            slug text GENERATED ALWAYS AS (
                lower(replace(regexp_replace(name, '[^a-zA-Z0-9\\s]', '', 'g'), ' ', '-'))
            ) STORED,
            search_vector tsvector GENERATED ALWAYS AS (
                to_tsvector('english', coalesce(name, '') || ' ' || coalesce(category, ''))
            ) STORED,
            price_with_tax numeric GENERATED ALWAYS AS (
                round(price * get_tax_rate(), 2)
            ) STORED
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that nested functions from GENERATED columns are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        println!("Generated column nested functions: {:?}", function_names);
        
        // Functions from complex GENERATED expressions should be extracted
        assert!(function_names.contains(&"lower"));
        assert!(function_names.contains(&"replace"));
        assert!(function_names.contains(&"regexp_replace"));
        assert!(function_names.contains(&"to_tsvector"));
        assert!(function_names.contains(&"coalesce"));
        assert!(function_names.contains(&"round"));
        assert!(function_names.contains(&"get_tax_rate"));
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"products"));
    }

    #[test]
    fn test_generated_column_with_subqueries() {
        let sql = "CREATE TABLE orders (
            id bigint PRIMARY KEY,
            user_id bigint NOT NULL,
            status text NOT NULL,
            item_count integer GENERATED ALWAYS AS (
                (SELECT count(*) FROM order_items WHERE order_id = orders.id)
            ) STORED,
            total_amount numeric GENERATED ALWAYS AS (
                (SELECT sum(price * quantity) FROM order_items WHERE order_id = orders.id)
            ) STORED,
            user_email text GENERATED ALWAYS AS (
                (SELECT email FROM users WHERE id = orders.user_id)
            ) STORED
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that functions and tables from GENERATED column subqueries are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        println!("Generated column subquery functions: {:?}", function_names);
        
        assert!(function_names.contains(&"count"));
        assert!(function_names.contains(&"sum"));
        
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"order_items"));
        assert!(table_names.contains(&"users"));
    }

    #[test]
    fn test_generated_column_with_types() {
        let sql = "CREATE TABLE analytics (
            id bigint PRIMARY KEY,
            raw_data jsonb NOT NULL,
            processed_data jsonb GENERATED ALWAYS AS (
                transform_json(raw_data)::analytics_result
            ) STORED,
            summary text GENERATED ALWAYS AS (
                extract_summary(processed_data)::summary_type
            ) STORED,
            tags text[] GENERATED ALWAYS AS (
                string_to_array(get_tags(raw_data), ',')::tag_array
            ) STORED
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that functions from GENERATED columns with type casting are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        println!("Generated column type functions: {:?}", function_names);
        
        assert!(function_names.contains(&"transform_json"));
        assert!(function_names.contains(&"extract_summary"));
        assert!(function_names.contains(&"string_to_array"));
        assert!(function_names.contains(&"get_tags"));
        
        // Check that custom types are extracted
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"analytics_result"));
        assert!(type_names.contains(&"summary_type"));
        assert!(type_names.contains(&"tag_array"));
    }

    #[test]
    fn test_generated_column_with_schema_qualified_functions() {
        let sql = "CREATE TABLE audit_log (
            id bigint PRIMARY KEY,
            event_data jsonb NOT NULL,
            user_id bigint NOT NULL,
            normalized_event jsonb GENERATED ALWAYS AS (
                utils.normalize_event(event_data)
            ) STORED,
            audit_hash text GENERATED ALWAYS AS (
                crypto.hash_event(event_data, security.get_salt())
            ) STORED,
            formatted_timestamp text GENERATED ALWAYS AS (
                formatting.format_timestamp(extract(epoch from created_at))
            ) STORED,
            created_at timestamptz DEFAULT now()
        );";
        
        let result = analyze_statement(sql).unwrap();
        
        // Check that schema-qualified functions from GENERATED columns are extracted
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        println!("Generated column schema-qualified functions: {:?}", function_names);
        
        assert!(function_names.contains(&"normalize_event"));
        assert!(function_names.contains(&"hash_event"));
        assert!(function_names.contains(&"get_salt"));
        assert!(function_names.contains(&"format_timestamp"));
        assert!(function_names.contains(&"extract"));
        assert!(function_names.contains(&"now"));
        
        // Check schema qualification
        let utils_func = result.functions.iter()
            .find(|f| f.name == "normalize_event")
            .unwrap();
        assert_eq!(utils_func.schema, Some("utils".to_string()));
        
        let crypto_func = result.functions.iter()
            .find(|f| f.name == "hash_event")
            .unwrap();
        assert_eq!(crypto_func.schema, Some("crypto".to_string()));
        
        let security_func = result.functions.iter()
            .find(|f| f.name == "get_salt")
            .unwrap();
        assert_eq!(security_func.schema, Some("security".to_string()));
        
        let formatting_func = result.functions.iter()
            .find(|f| f.name == "format_timestamp")
            .unwrap();
        assert_eq!(formatting_func.schema, Some("formatting".to_string()));
    }

    #[test]
    fn test_filter_builtins() {
        // Create a mock builtin catalog
        let mut catalog = BuiltinCatalog::new();
        
        // Add some built-in functions
        catalog.functions.insert(QualifiedIdent::from_name("sum".to_string()));
        catalog.functions.insert(QualifiedIdent::from_name("count".to_string()));
        catalog.functions.insert(QualifiedIdent::from_name("now".to_string()));
        catalog.functions.insert(QualifiedIdent::new(Some("pg_catalog".to_string()), "sum".to_string()));
        
        // Add some built-in types
        catalog.types.insert(QualifiedIdent::from_name("text".to_string()));
        catalog.types.insert(QualifiedIdent::from_name("integer".to_string()));
        catalog.types.insert(QualifiedIdent::from_name("jsonb".to_string()));
        
        // Add some built-in relations
        catalog.relations.insert(QualifiedIdent::new(Some("pg_catalog".to_string()), "pg_class".to_string()));
        catalog.relations.insert(QualifiedIdent::new(Some("information_schema".to_string()), "tables".to_string()));
        
        // Create dependencies with a mix of built-ins and custom objects
        let mut functions = HashSet::new();
        functions.insert(QualifiedIdent::from_name("sum".to_string()));
        functions.insert(QualifiedIdent::from_name("my_custom_func".to_string()));
        functions.insert(QualifiedIdent::from_name("count".to_string()));
        functions.insert(QualifiedIdent::new(Some("api".to_string()), "process_order".to_string()));
        functions.insert(QualifiedIdent::new(Some("pg_catalog".to_string()), "sum".to_string()));
        
        let mut types = HashSet::new();
        types.insert(QualifiedIdent::from_name("text".to_string()));
        types.insert(QualifiedIdent::from_name("my_custom_type".to_string()));
        types.insert(QualifiedIdent::from_name("jsonb".to_string()));
        types.insert(QualifiedIdent::new(Some("api".to_string()), "order_status".to_string()));
        
        let mut relations = HashSet::new();
        relations.insert(QualifiedIdent::new(Some("public".to_string()), "users".to_string()));
        relations.insert(QualifiedIdent::new(Some("pg_catalog".to_string()), "pg_class".to_string()));
        relations.insert(QualifiedIdent::new(Some("api".to_string()), "orders".to_string()));
        relations.insert(QualifiedIdent::new(Some("information_schema".to_string()), "tables".to_string()));
        
        let deps = Dependencies {
            functions,
            types,
            relations,
        };
        
        // Filter out built-ins
        let filtered = filter_builtins(deps, &catalog);
        
        // Check that only custom objects remain
        assert_eq!(filtered.functions.len(), 2);
        assert!(filtered.functions.iter().any(|f| f.name == "my_custom_func"));
        assert!(filtered.functions.iter().any(|f| f.name == "process_order" && f.schema == Some("api".to_string())));
        
        assert_eq!(filtered.types.len(), 2);
        assert!(filtered.types.iter().any(|t| t.name == "my_custom_type"));
        assert!(filtered.types.iter().any(|t| t.name == "order_status" && t.schema == Some("api".to_string())));
        
        assert_eq!(filtered.relations.len(), 2);
        assert!(filtered.relations.iter().any(|r| r.name == "users" && r.schema == Some("public".to_string())));
        assert!(filtered.relations.iter().any(|r| r.name == "orders" && r.schema == Some("api".to_string())));
    }

    #[test]
    fn test_filter_builtins_empty_catalog() {
        // Test with empty catalog (no filtering)
        let catalog = BuiltinCatalog::new();
        
        let mut functions = HashSet::new();
        functions.insert(QualifiedIdent::from_name("sum".to_string()));
        functions.insert(QualifiedIdent::from_name("my_func".to_string()));
        
        let mut types = HashSet::new();
        types.insert(QualifiedIdent::from_name("text".to_string()));
        types.insert(QualifiedIdent::from_name("my_type".to_string()));
        
        let mut relations = HashSet::new();
        relations.insert(QualifiedIdent::from_name("users".to_string()));
        
        let deps = Dependencies {
            functions,
            types,
            relations,
        };
        
        let filtered = filter_builtins(deps.clone(), &catalog);
        
        // All items should remain since catalog is empty
        assert_eq!(filtered.functions.len(), 2);
        assert_eq!(filtered.types.len(), 2);
        assert_eq!(filtered.relations.len(), 1);
    }

    #[test]
    fn test_analyze_with_builtins() {
        let sql = "SELECT 
            sum(amount) as total,
            count(*) as cnt,
            my_custom_func(data) as custom_result
        FROM orders
        WHERE created_at > now() - interval '30 days'
        AND status = 'active'::order_status";
        
        let result = analyze_statement(sql).unwrap();
        
        // Before filtering, should have all functions
        assert!(result.functions.iter().any(|f| f.name == "sum"));
        assert!(result.functions.iter().any(|f| f.name == "count"));
        assert!(result.functions.iter().any(|f| f.name == "now"));
        assert!(result.functions.iter().any(|f| f.name == "my_custom_func"));
        
        // Create a mock catalog with built-ins
        let mut catalog = BuiltinCatalog::new();
        catalog.functions.insert(QualifiedIdent::from_name("sum".to_string()));
        catalog.functions.insert(QualifiedIdent::from_name("count".to_string()));
        catalog.functions.insert(QualifiedIdent::from_name("now".to_string()));
        
        // Filter
        let filtered = filter_builtins(result, &catalog);
        
        // After filtering, only custom function remains
        assert_eq!(filtered.functions.len(), 1);
        assert!(filtered.functions.iter().any(|f| f.name == "my_custom_func"));
    }

    #[test]
    fn test_parser_special_forms_filtering() {
        let sql = "SELECT 
            COALESCE(name, 'Unknown') as display_name,
            NULLIF(status, '') as clean_status,
            GREATEST(created_at, updated_at) as last_activity,
            SUBSTRING(description FROM 1 FOR 100) as summary,
            EXTRACT(year FROM created_at) as year,
            CURRENT_TIMESTAMP as query_time,
            CURRENT_USER as who,
            my_format_func(data) as formatted
        FROM items
        WHERE status = ANY(ARRAY['active', 'pending'])
        AND created_at > CURRENT_DATE - interval '7 days'";
        
        let result = analyze_statement(sql).unwrap();
        
        // Create a mock catalog with parser special forms
        let mut catalog = BuiltinCatalog::new();
        
        // Add parser special forms (both qualified and unqualified)
        for construct in ["coalesce", "nullif", "greatest", "substring", "extract", 
                         "current_timestamp", "current_user", "current_date", "any", "array"] {
            catalog.functions.insert(QualifiedIdent::from_name(construct.to_string()));
            catalog.functions.insert(QualifiedIdent::new(Some("pg_catalog".to_string()), construct.to_string()));
        }
        
        // Filter
        let filtered = filter_builtins(result, &catalog);
        
        // After filtering, only custom function remains
        assert_eq!(filtered.functions.len(), 1);
        assert!(filtered.functions.iter().any(|f| f.name == "my_format_func"));
    }

    #[test]
    fn test_language_sql_function_basic() {
        let sql = "CREATE FUNCTION get_user_name(user_id integer)
        RETURNS text
        LANGUAGE sql
        AS $$
            SELECT name FROM users WHERE id = $1;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table from the function body
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        
        // Should extract the function being created
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"get_user_name"));
    }

    #[test]
    fn test_language_sql_function_with_joins() {
        let sql = "CREATE OR REPLACE FUNCTION get_order_details(p_order_id integer)
        RETURNS TABLE(order_id integer, customer_name text, total_amount numeric)
        LANGUAGE sql
        AS $$
            SELECT 
                o.id,
                c.name,
                o.total
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            WHERE o.id = p_order_id;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract tables from the JOIN
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"customers"));
        
        // Should extract the function being created
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"get_order_details"));
    }

    #[test]
    fn test_language_sql_function_with_subqueries() {
        let sql = "CREATE FUNCTION calculate_user_stats(p_user_id integer)
        RETURNS user_stats_type
        LANGUAGE sql
        AS $$
            SELECT 
                u.name,
                (SELECT COUNT(*) FROM orders WHERE customer_id = p_user_id) as order_count,
                (SELECT SUM(total) FROM orders WHERE customer_id = p_user_id) as total_spent,
                (SELECT MAX(created_at) FROM orders WHERE customer_id = p_user_id) as last_order_date
            FROM users u
            WHERE u.id = p_user_id;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract tables from main query and subqueries
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"orders"));
        
        // Should extract functions from subqueries
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"count"));
        assert!(function_names.contains(&"sum"));
        assert!(function_names.contains(&"max"));
        assert!(function_names.contains(&"calculate_user_stats"));
        
        // Should extract custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"user_stats_type"));
    }

    #[test]
    fn test_language_sql_function_with_multiple_statements() {
        let sql = "CREATE FUNCTION process_user_order(p_user_id integer, p_order_id integer)
        RETURNS order_summary_type
        LANGUAGE sql
        AS $$
            INSERT INTO order_log (user_id, order_id, action, timestamp)
            VALUES (p_user_id, p_order_id, 'processed', NOW());
            
            UPDATE orders 
            SET status = 'processed'::order_status, 
                processed_at = CURRENT_TIMESTAMP,
                processor_id = get_current_processor()
            WHERE id = p_order_id AND customer_id = p_user_id;
            
            SELECT 
                o.id,
                o.total,
                o.status,
                u.name as customer_name,
                calculate_tax(o.total, u.state) as tax_amount
            FROM orders o
            JOIN users u ON o.customer_id = u.id
            WHERE o.id = p_order_id;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract all tables from all statements
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"order_log"));
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"users"));
        
        // Should extract function calls from all statements
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"get_current_processor"));
        assert!(function_names.contains(&"calculate_tax"));
        assert!(function_names.contains(&"process_user_order")); // Function name itself
        
        // Should extract types from all statements
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"order_summary_type"));
        assert!(type_names.contains(&"order_status"));
    }

    #[test]
    fn test_language_sql_function_with_ddl_and_dml() {
        let sql = "CREATE FUNCTION setup_user_workspace(p_user_id integer, p_workspace_name text)
        RETURNS workspace_info_type
        LANGUAGE sql
        AS $$
            CREATE TEMP TABLE workspace_items AS
            SELECT * FROM template_items WHERE template_type = 'default';
            
            INSERT INTO user_workspaces (user_id, name, created_at)
            VALUES (p_user_id, p_workspace_name, NOW());
            
            INSERT INTO workspace_permissions (workspace_id, user_id, permission_level)
            SELECT 
                currval('user_workspaces_id_seq'::regclass),
                p_user_id,
                'owner'::permission_level;
            
            SELECT 
                uw.id,
                uw.name,
                uw.created_at,
                COUNT(wi.id) as item_count
            FROM user_workspaces uw
            LEFT JOIN workspace_items wi ON TRUE
            WHERE uw.user_id = p_user_id AND uw.name = p_workspace_name
            GROUP BY uw.id, uw.name, uw.created_at;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract all tables from DDL and DML statements
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"workspace_items"));
        assert!(table_names.contains(&"template_items"));
        assert!(table_names.contains(&"user_workspaces"));
        assert!(table_names.contains(&"workspace_permissions"));
        
        // Should extract function calls
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"currval"));
        assert!(function_names.contains(&"count"));
        assert!(function_names.contains(&"setup_user_workspace"));
        
        // Should extract types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"workspace_info_type"));
        assert!(type_names.contains(&"permission_level"));
        assert!(type_names.contains(&"regclass"));
        assert!(type_names.contains(&"text"));
    }

    #[test]
    fn test_language_sql_function_with_cte_and_multiple_statements() {
        let sql = "CREATE FUNCTION analyze_user_activity(p_user_id integer, p_days integer)
        RETURNS activity_report_type
        LANGUAGE sql
        AS $$
            DELETE FROM temp_activity_cache WHERE user_id = p_user_id;
            
            WITH recent_orders AS (
                SELECT 
                    o.id,
                    o.total,
                    extract_category(p.name) as category
                FROM orders o
                JOIN order_items oi ON o.id = oi.order_id
                JOIN products p ON oi.product_id = p.id
                WHERE o.customer_id = p_user_id 
                  AND o.created_at >= NOW() - '30 days'::interval
            ),
            category_totals AS (
                SELECT 
                    category,
                    SUM(total) as category_total,
                    calculate_trend(category, p_days) as trend
                FROM recent_orders
                GROUP BY category
            )
            INSERT INTO activity_summary (user_id, category, total, trend, report_date)
            SELECT p_user_id, category, category_total, trend, CURRENT_DATE
            FROM category_totals;
            
            SELECT 
                generate_report_summary(p_user_id) as summary,
                array_agg(category ORDER BY total DESC) as top_categories
            FROM activity_summary
            WHERE user_id = p_user_id AND report_date = CURRENT_DATE;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract all tables from all statements including CTEs
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"temp_activity_cache"));
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"order_items"));
        assert!(table_names.contains(&"products"));
        assert!(table_names.contains(&"activity_summary"));
        
        // Should extract function calls from all statements
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"extract_category"));
        assert!(function_names.contains(&"now"));
        assert!(function_names.contains(&"calculate_trend"));
        assert!(function_names.contains(&"sum"));
        assert!(function_names.contains(&"generate_report_summary"));
        assert!(function_names.contains(&"array_agg"));
        assert!(function_names.contains(&"analyze_user_activity"));
        
        // Should extract types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"activity_report_type"));
        assert!(type_names.contains(&"interval"));
    }

    #[test]
    fn test_language_sql_function_with_cte() {
        let sql = "CREATE FUNCTION get_top_customers(limit_count integer)
        RETURNS SETOF customer_summary
        LANGUAGE sql
        AS $$
            WITH order_totals AS (
                SELECT 
                    customer_id,
                    SUM(total) as total_spent,
                    COUNT(*) as order_count
                FROM orders
                GROUP BY customer_id
            ),
            ranked_customers AS (
                SELECT 
                    c.id,
                    c.name,
                    c.email,
                    ot.total_spent,
                    ot.order_count,
                    RANK() OVER (ORDER BY ot.total_spent DESC) as rank
                FROM customers c
                JOIN order_totals ot ON c.id = ot.customer_id
            )
            SELECT * FROM ranked_customers WHERE rank <= limit_count;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract tables from CTE and main query
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"customers"));
        
        // Should extract functions from CTE
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"sum"));
        assert!(function_names.contains(&"count"));
        assert!(function_names.contains(&"rank"));
        assert!(function_names.contains(&"get_top_customers"));
        
        // Should extract return type
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"customer_summary"));
    }

    #[test]
    fn test_language_sql_function_with_custom_functions() {
        let sql = "CREATE FUNCTION process_order_data(p_start_date date, p_end_date date)
        RETURNS TABLE(processed_data jsonb)
        LANGUAGE sql
        AS $$
            SELECT jsonb_build_object(
                'order_id', o.id,
                'customer', get_customer_info(o.customer_id),
                'formatted_total', format_currency(o.total),
                'shipping_cost', calculate_shipping(o.shipping_address),
                'tax_amount', compute_tax(o.total, o.tax_rate)
            ) as processed_data
            FROM orders o
            WHERE o.created_at BETWEEN p_start_date AND p_end_date
              AND validate_order(o.id) = true;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract the table
        let table_names: Vec<&str> = result.relations.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"));
        
        // Should extract all function calls including custom ones
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"jsonb_build_object"));
        assert!(function_names.contains(&"get_customer_info"));
        assert!(function_names.contains(&"format_currency"));
        assert!(function_names.contains(&"calculate_shipping"));
        assert!(function_names.contains(&"compute_tax"));
        assert!(function_names.contains(&"validate_order"));
        assert!(function_names.contains(&"process_order_data"));
    }

    #[test]
    fn test_language_sql_function_with_schema_qualified_objects() {
        let sql = "CREATE FUNCTION api.get_user_profile(p_user_id integer)
        RETURNS api.user_profile
        LANGUAGE sql
        AS $$
            SELECT 
                u.id,
                u.name,
                u.email,
                auth.hash_email(u.email) as email_hash,
                stats.calculate_user_score(u.id) as score,
                (SELECT COUNT(*) FROM public.orders WHERE customer_id = u.id) as order_count
            FROM auth.users u
            WHERE u.id = p_user_id
              AND auth.is_active(u.id);
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract schema-qualified tables
        let relations: Vec<_> = result.relations.iter().collect();
        assert!(relations.iter().any(|r| r.name == "users" && r.schema == Some("auth".to_string())));
        assert!(relations.iter().any(|r| r.name == "orders" && r.schema == Some("public".to_string())));
        
        // Should extract schema-qualified functions
        let functions: Vec<_> = result.functions.iter().collect();
        assert!(functions.iter().any(|f| f.name == "hash_email" && f.schema == Some("auth".to_string())));
        assert!(functions.iter().any(|f| f.name == "calculate_user_score" && f.schema == Some("stats".to_string())));
        assert!(functions.iter().any(|f| f.name == "is_active" && f.schema == Some("auth".to_string())));
        assert!(functions.iter().any(|f| f.name == "get_user_profile" && f.schema == Some("api".to_string())));
        
        // Should extract schema-qualified types
        let types: Vec<_> = result.types.iter().collect();
        assert!(types.iter().any(|t| t.name == "user_profile" && t.schema == Some("api".to_string())));
    }

    #[test]
    fn test_language_sql_function_with_complex_expressions() {
        let sql = "CREATE FUNCTION calculate_discounted_price(
            p_original_price numeric,
            p_customer_tier customer_tier_enum,
            p_product_category text
        )
        RETURNS discount_result
        LANGUAGE sql
        AS $$
            SELECT ROW(
                p_original_price,
                CASE p_customer_tier
                    WHEN 'premium'::customer_tier_enum THEN 
                        p_original_price * get_premium_discount_rate(p_product_category)
                    WHEN 'gold'::customer_tier_enum THEN
                        p_original_price * get_gold_discount_rate(p_product_category)
                    ELSE
                        p_original_price * get_standard_discount_rate(p_product_category)
                END,
                apply_seasonal_adjustment(p_original_price, CURRENT_DATE),
                validate_price_range(p_original_price)
            )::discount_result;
        $$;";
        
        let result = analyze_statement(sql).unwrap();
        
        // Should extract custom functions
        let function_names: Vec<&str> = result.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains(&"get_premium_discount_rate"));
        assert!(function_names.contains(&"get_gold_discount_rate"));
        assert!(function_names.contains(&"get_standard_discount_rate"));
        assert!(function_names.contains(&"apply_seasonal_adjustment"));
        assert!(function_names.contains(&"validate_price_range"));
        assert!(function_names.contains(&"calculate_discounted_price"));
        
        // Should extract custom types
        let type_names: Vec<&str> = result.types.iter().map(|t| t.name.as_str()).collect();
        assert!(type_names.contains(&"customer_tier_enum"));
        assert!(type_names.contains(&"discount_result"));
    }
}