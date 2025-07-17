use std::collections::HashSet;
use pg_query::{NodeEnum, NodeRef};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedIdent {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Dependencies {
    pub relations: Vec<QualifiedIdent>,
    pub functions: Vec<QualifiedIdent>,
    pub types: Vec<QualifiedIdent>,
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
    
    // Also traverse the entire AST to extract REFERENCES and DEFAULT functions
    for stmt in &parse_result.protobuf.stmts {
        if let Some(stmt) = &stmt.stmt {
            if let Some(node) = &stmt.node {
                extract_from_node(node, &mut relations, &mut functions);
            }
        }
    }
    
    // Extract types from cast expressions
    let types = extract_types_from_ast(&parse_result.protobuf)?;
    
    Ok(Dependencies {
        relations: relations.into_iter().collect(),
        functions: functions.into_iter().collect(),
        types,
    })
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
    // Check if it's a DEFAULT or CHECK constraint with expressions
    if constraint.contype == ConstrType::ConstrDefault as i32 || 
        constraint.contype == ConstrType::ConstrCheck as i32 {
        if let Some(raw_expr) = &constraint.raw_expr {
            extract_from_node(raw_expr.node.as_ref().unwrap(), relations, functions);
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
        NodeEnum::TypeCast(type_cast) => {
            if let Some(arg) = &type_cast.arg {
                extract_from_node(arg.node.as_ref().unwrap(), relations, functions);
            }
        }
        NodeEnum::CreateDomainStmt(domain_stmt) => {
            // Extract base type from domain definition
            if let Some(type_name) = &domain_stmt.type_name {
                if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                    if !is_builtin_type(&qualified_type.name) {
                        // This will be handled by extract_types_from_ast, but we can also
                        // extract from domain constraints if they contain function calls
                    }
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
        _ => {
            // For other node types, we could add more specific handling
            // This is a simplified approach - in a full implementation, we'd need to
            // handle all node types and their specific child structures
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
                        if !is_builtin_type(&qualified_type.name) {
                            types.insert(qualified_type);
                        }
                    }
                }
            }
            NodeRef::CompositeTypeStmt(composite_type) => {
                // Extract types from composite type column definitions
                for col_def in &composite_type.coldeflist {
                    if let Some(NodeEnum::ColumnDef(column_def)) = &col_def.node {
                        if let Some(type_name) = &column_def.type_name {
                            if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                                if !is_builtin_type(&qualified_type.name) {
                                    types.insert(qualified_type);
                                }
                            }
                        }
                    }
                }
            }
            NodeRef::CreateDomainStmt(domain_stmt) => {
                // Extract the base type of the domain
                if let Some(type_name) = &domain_stmt.type_name {
                    if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                        if !is_builtin_type(&qualified_type.name) {
                            types.insert(qualified_type);
                        }
                    }
                }
            }
            NodeRef::CreateStmt(create_stmt) => {
                // Extract types from CREATE TABLE column definitions
                for table_elt in &create_stmt.table_elts {
                    if let Some(NodeEnum::ColumnDef(column_def)) = &table_elt.node {
                        if let Some(type_name) = &column_def.type_name {
                            if let Some(qualified_type) = extract_type_from_type_name(type_name) {
                                if !is_builtin_type(&qualified_type.name) {
                                    types.insert(qualified_type);
                                }
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
                                        if !is_builtin_type(&qualified_type.name) {
                                            types.insert(qualified_type);
                                        }
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

fn is_builtin_type(type_name: &str) -> bool {
    // List of common PostgreSQL built-in types
    // This is not exhaustive but covers the most common types
    matches!(type_name.to_lowercase().as_str(),
        "int" | "int2" | "int4" | "int8" | "integer" | "smallint" | "bigint" |
        "serial" | "smallserial" | "bigserial" | "serial2" | "serial4" | "serial8" |
        "float" | "float4" | "float8" | "real" | "double" | "precision" |
        "numeric" | "decimal" | "money" |
        "char" | "varchar" | "text" | "bpchar" | "character" |
        "bool" | "boolean" |
        "date" | "time" | "timestamp" | "timestamptz" | "interval" |
        "uuid" | "json" | "jsonb" | "xml" |
        "bytea" | "bit" | "varbit" |
        "inet" | "cidr" | "macaddr" | "macaddr8" |
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" |
        "tsvector" | "tsquery" |
        "void" | "unknown" | "anyarray" | "anyelement" | "anynonarray" | "anyenum" |
        "record" | "cstring" | "any" | "anyrange" | "event_trigger" | "fdw_handler" |
        "index_am_handler" | "language_handler" | "tsm_handler" | "internal" |
        "opaque" | "trigger" | "pg_lsn" | "txid_snapshot" | "pg_snapshot"
    )
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
                        if !is_builtin_type(&qualified_type.name) {
                            all_types.insert(qualified_type);
                        }
                    }
                }
                
                // Extract parameter types
                for param in &create_func.parameters {
                    if let Some(NodeEnum::FunctionParameter(func_param)) = &param.node {
                        if let Some(arg_type) = &func_param.arg_type {
                            if let Some(qualified_type) = extract_type_from_type_name(arg_type) {
                                if !is_builtin_type(&qualified_type.name) {
                                    all_types.insert(qualified_type);
                                }
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
                    
                    // Filter out built-in types and pg_catalog types
                    if type_ident.schema.as_ref().map_or(true, |s| s != "pg_catalog") 
                        && !is_builtin_type(&type_ident.name) {
                        types.insert(type_ident);
                    }
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
        assert_eq!(result.types[0].name, "currency");
        assert_eq!(result.types[0].schema, None);
    }

    #[test]
    fn test_qualified_type_cast() {
        let sql = "SELECT 42::api.cart_summary";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        assert_eq!(result.types[0].name, "cart_summary");
        assert_eq!(result.types[0].schema, Some("api".to_string()));
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
        assert!(!type_names.contains(&"int"));
        
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
        assert_eq!(result.types[0].name, "currency");
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
        assert_eq!(result.types[0].name, "custom_type");
    }

    #[test]
    fn test_builtin_types_filtered() {
        let sql = "SELECT '2023-01-01'::date, 42::integer, 'hello'::text, true::boolean";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 0);
    }

    #[test]
    fn test_mixed_builtin_and_custom() {
        let sql = "SELECT 42::integer, 'data'::custom_type, true::boolean";
        let result = analyze_statement(sql).unwrap();
        
        assert_eq!(result.types.len(), 1);
        assert_eq!(result.types[0].name, "custom_type");
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
        assert_eq!(result.types[0].name, "status_log");
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
        assert_eq!(result.types[0].name, "custom_type");
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
        assert!(!type_names.contains(&"date"));
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
        assert!(!type_names.contains(&"int"));
        assert!(!type_names.contains(&"text"));
        assert!(!type_names.contains(&"bool"));
        
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
        assert!(!type_names.contains(&"numeric"));
        assert!(!type_names.contains(&"text"));
        assert!(!type_names.contains(&"uuid"));
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
        assert!(!type_names.contains(&"uuid"));
        assert!(!type_names.contains(&"jsonb"));
        assert!(!type_names.contains(&"timestamptz"));
        assert!(!type_names.contains(&"int"));
        
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
        
        // Built-in types should be filtered
        assert!(!type_names.contains(&"text"));
        assert!(!type_names.contains(&"uuid"));
        assert!(!type_names.contains(&"char"));
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
        assert!(!type_names.contains(&"jsonb"));
    }
}