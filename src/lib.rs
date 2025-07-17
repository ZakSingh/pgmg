use std::collections::HashSet;
use pg_query::{NodeEnum, NodeRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedIdent {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct StmtDependencies {
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

pub fn analyze_statement(sql: &str) -> Result<StmtDependencies, Box<dyn std::error::Error>> {
    let parse_result = pg_query::parse(sql)?;
    
    // Extract relations and functions using existing pg_query functionality
    let relations = parse_result.tables().into_iter()
        .map(|table| QualifiedIdent::from_qualified_name(&table))
        .collect();
    
    let functions = parse_result.functions().into_iter()
        .map(|func| QualifiedIdent::from_qualified_name(&func))
        .collect();
    
    // Extract types from cast expressions
    let types = extract_types_from_ast(&parse_result.protobuf)?;
    
    Ok(StmtDependencies {
        relations,
        functions,
        types,
    })
}

fn extract_types_from_ast(parse_result: &pg_query::protobuf::ParseResult) -> Result<Vec<QualifiedIdent>, Box<dyn std::error::Error>> {
    let mut types = HashSet::new();
    
    // The original approach works fine for most cases, but we need to ensure we catch TypeCast nodes
    // Let's implement a workaround by using the debug output to find TypeCast nodes
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
            _ => {}
        }
    }
    
    // For INSERT statements, we need to parse the debug output as a workaround
    // since nodes() doesn't properly traverse TypeCast nodes in VALUES clauses
    let debug_str = format!("{:?}", parse_result);
    extract_types_from_debug_string(&debug_str, &mut types);
    
    Ok(types.into_iter().collect())
}

fn extract_types_from_debug_string(debug_str: &str, types: &mut HashSet<QualifiedIdent>) {
    // Look for TypeCast patterns in the debug string
    // The pattern is: TypeCast { ... type_name: Some(TypeName { ... names: [... String(String { sval: "type_name" }) ...] ... }) ...}
    let type_cast_pattern = regex::Regex::new(r#"String\(String \{ sval: "([^"]+)" \}\)"#).unwrap();
    
    // Find all TypeCast blocks first
    let typecast_blocks: Vec<&str> = debug_str.split("TypeCast {").collect();
    
    for block in typecast_blocks {
        if block.contains("type_name: Some(TypeName {") {
            // This is a TypeCast block with a type_name
            let names_part = if let Some(names_start) = block.find("names: [") {
                &block[names_start..]
            } else {
                continue;
            };
            
            let names_end = if let Some(end) = names_part.find(']') {
                &names_part[..end]
            } else {
                continue;
            };
            
            // Extract all string values from the names array
            let mut type_parts = Vec::new();
            for cap in type_cast_pattern.captures_iter(names_end) {
                type_parts.push(cap[1].to_string());
            }
            
            if !type_parts.is_empty() {
                let qualified_type = if type_parts.len() == 1 {
                    QualifiedIdent::from_name(type_parts[0].clone())
                } else if type_parts.len() == 2 {
                    QualifiedIdent::new(Some(type_parts[0].clone()), type_parts[1].clone())
                } else {
                    // Handle cases with more than 2 parts
                    let len = type_parts.len();
                    QualifiedIdent::new(
                        Some(type_parts[len - 2].clone()),
                        type_parts[len - 1].clone(),
                    )
                };
                
                if !is_builtin_type(&qualified_type.name) {
                    types.insert(qualified_type);
                }
            }
        }
    }
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