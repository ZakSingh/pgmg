use crate::sql::{SqlObject, ObjectType};
use owo_colors::OwoColorize;

#[derive(Debug, Clone)]
pub struct PlpgsqlCheckResult {
    pub functionid: Option<String>,
    pub lineno: Option<i32>,
    pub statement: Option<String>,
    pub sqlstate: Option<String>,
    pub message: Option<String>,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub level: Option<String>,
    pub position: Option<i32>,
    pub query: Option<String>,
    pub context: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PlpgsqlCheckError {
    pub function_name: String,
    pub source_file: Option<String>,
    pub source_line: Option<usize>,
    pub check_result: PlpgsqlCheckResult,
}

/// Check if the plpgsql_check extension is installed
pub async fn is_plpgsql_check_available<C>(client: &C) -> Result<bool, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
    let result = client.query_one(
        "SELECT EXISTS (
            SELECT 1 FROM pg_extension 
            WHERE extname = 'plpgsql_check'
        )",
        &[]
    ).await?;
    
    Ok(result.get(0))
}

/// Run plpgsql_check on all functions using the bulk query approach.
/// Returns (results, functions_examined) — `functions_examined` is the count of
/// eligible PL/pgSQL functions plpgsql_check ran against, which is needed because
/// clean functions return zero rows from plpgsql_check_function_tb().
pub async fn check_all_functions<C>(
    client: &C,
    schema_filter: Option<&[String]>,
    function_name_filter: Option<&str>,
) -> Result<(Vec<PlpgsqlCheckResult>, usize), Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
    // Shared FROM/WHERE used by both the check query and the count query.
    let from_clause = "
        FROM pg_proc
             LEFT JOIN pg_trigger
                       ON (pg_trigger.tgfoid = pg_proc.oid)
        WHERE
          prolang = (SELECT lang.oid FROM pg_language lang WHERE lang.lanname = 'plpgsql') AND
          pronamespace <> (SELECT nsp.oid FROM pg_namespace nsp WHERE nsp.nspname = 'pg_catalog') AND
          pg_proc.oid NOT IN (
              SELECT objid FROM pg_depend
              WHERE deptype = 'e'
              AND classid = 'pg_proc'::regclass
          )";

    // Build dynamic WHERE conditions
    let mut where_conditions = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> = Vec::new();
    let mut param_index = 1;

    // Add schema filtering
    if let Some(schemas) = schema_filter {
        if !schemas.is_empty() {
            where_conditions.push(format!("AND pronamespace IN (SELECT oid FROM pg_namespace WHERE nspname = ANY(${}))", param_index));
            params.push(Box::new(schemas.to_vec()));
            param_index += 1;
        }
    } else {
        // Default: exclude pg_* and information_schema
        where_conditions.push("AND pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema')".to_string());
    }

    // Add function name filtering
    if let Some(function_name) = function_name_filter {
        // Parse schema-qualified function names
        if function_name.contains('.') {
            let parts: Vec<&str> = function_name.splitn(2, '.').collect();
            let schema_name = parts[0].to_string();
            let func_name = parts[1].trim_end_matches("()").to_string();
            where_conditions.push(format!("AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = ${}) AND proname = ${}", param_index, param_index + 1));
            params.push(Box::new(schema_name));
            params.push(Box::new(func_name));
        } else {
            let func_name = function_name.trim_end_matches("()").to_string();
            where_conditions.push(format!("AND proname = ${}", param_index));
            params.push(Box::new(func_name));
        }
    } else {
        // When checking all functions, exclude internal ones starting with underscore
        where_conditions.push("AND proname NOT LIKE '\\_%'".to_string());
    }

    let trigger_filter = "AND (pg_proc.prorettype <> (SELECT typ.oid FROM pg_type typ WHERE typ.typname = 'trigger') OR
                               pg_trigger.tgfoid IS NOT NULL)";

    let full_query = format!(
        "SELECT
          (pcf).functionid::regprocedure::text, (pcf).lineno, (pcf).statement,
          (pcf).sqlstate, (pcf).message, (pcf).detail, (pcf).hint, (pcf).level,
          (pcf).\"position\", (pcf).query, (pcf).context
        FROM
          (
            SELECT
              plpgsql_check_function_tb(pg_proc.oid, COALESCE(pg_trigger.tgrelid, 0),
                                        oldtable=>pg_trigger.tgoldtable,
                                        newtable=>pg_trigger.tgnewtable) AS pcf
            {from_clause}
              {extra_where}
              {trigger_filter}
            OFFSET 0
          ) ss
        ORDER BY (pcf).functionid::regprocedure::text, (pcf).lineno",
        from_clause = from_clause,
        extra_where = where_conditions.join(" "),
        trigger_filter = trigger_filter,
    );

    // Count the eligible functions separately — plpgsql_check_function_tb returns
    // zero rows for clean functions, so we can't infer the total from results.
    let count_query = format!(
        "SELECT COUNT(DISTINCT pg_proc.oid)
        {from_clause}
          {extra_where}
          {trigger_filter}",
        from_clause = from_clause,
        extra_where = where_conditions.join(" "),
        trigger_filter = trigger_filter,
    );

    let (rows, count_row) = if params.is_empty() {
        let empty: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[];
        let rows = client.query(&full_query, empty).await?;
        let count_row = client.query_one(&count_query, empty).await?;
        (rows, count_row)
    } else {
        let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let rows = client.query(&full_query, &params_refs).await?;
        let count_row = client.query_one(&count_query, &params_refs).await?;
        (rows, count_row)
    };

    let functions_examined: i64 = count_row.get(0);
    let functions_examined = usize::try_from(functions_examined).unwrap_or(0);

    let mut results = Vec::new();

    for row in rows {
        // Parse the table output
        let result = PlpgsqlCheckResult {
            functionid: row.get::<_, Option<String>>(0),
            lineno: row.get::<_, Option<i32>>(1),
            statement: row.get::<_, Option<String>>(2),
            sqlstate: row.get::<_, Option<String>>(3),
            message: row.get::<_, Option<String>>(4),
            detail: row.get::<_, Option<String>>(5),
            hint: row.get::<_, Option<String>>(6),
            level: row.get::<_, Option<String>>(7),
            position: row.get::<_, Option<i32>>(8),
            query: row.get::<_, Option<String>>(9),
            context: row.get::<_, Option<String>>(10),
        };

        // Only include results with actual messages (skip empty rows)
        if result.level.is_some() && result.message.is_some() {
            results.push(result);
        }
    }

    Ok((results, functions_examined))
}

/// Check all functions that were created or updated using the bulk query approach
pub async fn check_modified_functions<C>(
    client: &C,
    modified_objects: &[&SqlObject],
) -> Result<Vec<PlpgsqlCheckError>, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
    let mut errors = Vec::new();
    
    // Filter to only functions and procedures (both can contain PL/pgSQL code)
    let functions: Vec<_> = modified_objects.iter()
        .filter(|obj| matches!(obj.object_type, ObjectType::Function | ObjectType::Procedure))
        .collect();
    
    if functions.is_empty() {
        return Ok(errors);
    }
    
    // Check if extension is available
    if !is_plpgsql_check_available(client).await? {
        eprintln!("{}: plpgsql_check extension is not installed. Skipping function/procedure checks.", 
            "Warning".yellow().bold());
        return Ok(errors);
    }
    
    // Use bulk query to check all functions, then filter results
    let (all_results, _) = check_all_functions(client, None, None).await?;

    // Create a map of modified function names for quick lookup
    let mut modified_function_names = std::collections::HashSet::new();
    for function in &functions {
        let func_name = match &function.qualified_name.schema {
            Some(schema) => format!("{}.{}", schema, function.qualified_name.name),
            None => function.qualified_name.name.clone(),
        };
        modified_function_names.insert(func_name);
    }
    
    // Filter bulk results to only modified functions
    for result in all_results {
        if let Some(functionid) = &result.functionid {
            // Extract function name from regprocedure format (schema.function or just function)
            let function_name = if functionid.contains('(') {
                // Remove parameters from function signature
                functionid.split('(').next().unwrap_or(functionid).to_string()
            } else {
                functionid.clone()
            };
            
            // Check if this function was modified
            if modified_function_names.contains(&function_name) {
                // Only report errors and warnings (skip notices). plpgsql_check
                // emits levels like "warning extra"/"warning performance" — use prefix.
                if let Some(level) = &result.level {
                    if level.starts_with("error") || level.starts_with("warning") {
                        // Find the corresponding SqlObject for source file info
                        let source_info = functions.iter()
                            .find(|f| {
                                let obj_name = match &f.qualified_name.schema {
                                    Some(schema) => format!("{}.{}", schema, f.qualified_name.name),
                                    None => f.qualified_name.name.clone(),
                                };
                                obj_name == function_name
                            });
                        
                        let error = PlpgsqlCheckError {
                            function_name: function_name.clone(),
                            source_file: source_info.and_then(|f| f.source_file.as_ref().map(|p| p.to_string_lossy().to_string())),
                            source_line: source_info.and_then(|f| calculate_source_line(f, result.lineno)),
                            check_result: result,
                        };
                        errors.push(error);
                    }
                }
            }
        }
    }
    
    Ok(errors)
}

/// Check functions that have soft dependencies on modified functions
/// These are functions that call the modified functions and need validation
pub async fn check_soft_dependent_functions<C>(
    client: &C,
    dependency_graph: &crate::analysis::DependencyGraph,
    modified_objects: &[&SqlObject],
    all_file_objects: &[SqlObject],
) -> Result<Vec<PlpgsqlCheckError>, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
    use crate::analysis::ObjectRef;
    
    let mut errors = Vec::new();
    
    // Check if extension is available
    if !is_plpgsql_check_available(client).await? {
        return Ok(errors);
    }
    
    // Find all soft dependents of modified functions
    let mut functions_to_check = std::collections::HashSet::new();
    
    for modified_obj in modified_objects {
        if matches!(modified_obj.object_type, ObjectType::Function | ObjectType::Procedure) {
            let obj_ref = ObjectRef::from(*modified_obj);
            
            // Get all soft dependents (functions that call this function)
            for dependent in dependency_graph.soft_dependents_of(&obj_ref) {
                if matches!(dependent.object_type, ObjectType::Function | ObjectType::Procedure) {
                    functions_to_check.insert(dependent);
                }
            }
        }
    }
    
    if functions_to_check.is_empty() {
        return Ok(errors);
    }
    
    // Don't print status message here to avoid breaking output flow
    
    let num_functions_to_check = functions_to_check.len();
    
    // Use bulk query to check all functions, then filter to dependents
    let (all_results, _) = check_all_functions(client, None, None).await?;
    
    // Create a map of function names to check
    let mut dependent_function_names = std::collections::HashSet::new();
    for func_ref in &functions_to_check {
        let func_name = match &func_ref.qualified_name.schema {
            Some(schema) => format!("{}.{}", schema, func_ref.qualified_name.name),
            None => func_ref.qualified_name.name.clone(),
        };
        dependent_function_names.insert(func_name);
    }
    
    // Filter bulk results to only dependent functions
    for result in all_results {
        if let Some(functionid) = &result.functionid {
            // Extract function name from regprocedure format
            let function_name = if functionid.contains('(') {
                functionid.split('(').next().unwrap_or(functionid).to_string()
            } else {
                functionid.clone()
            };
            
            // Check if this is a dependent function we need to check
            if dependent_function_names.contains(&function_name) {
                // Only report errors (not warnings for dependent functions)
                if let Some(level) = &result.level {
                    if level.starts_with("error") {
                        // Find the corresponding SqlObject for source file info
                        let source_info = all_file_objects.iter()
                            .find(|f| {
                                let obj_name = match &f.qualified_name.schema {
                                    Some(schema) => format!("{}.{}", schema, f.qualified_name.name),
                                    None => f.qualified_name.name.clone(),
                                };
                                obj_name == function_name && matches!(f.object_type, ObjectType::Function | ObjectType::Procedure)
                            });
                        
                        let error = PlpgsqlCheckError {
                            function_name: function_name.clone(),
                            source_file: source_info.and_then(|f| f.source_file.as_ref().map(|p| p.to_string_lossy().to_string())),
                            source_line: source_info.and_then(|f| calculate_source_line(f, result.lineno)),
                            check_result: result,
                        };
                        errors.push(error);
                    }
                }
            }
        }
    }
    
    if errors.is_empty() && num_functions_to_check > 0 {
        println!("  {} All dependent functions remain compatible", "✓".green().bold());
    }
    
    Ok(errors)
}

/// Find the line offset (0-based, relative to the first line of the CREATE
/// statement) of the body's opening dollar-quote tag. Returns `None` if no
/// dollar-quoted body is found (e.g. SQL-language functions).
///
/// Searches for the first `AS $tag$` after a `LANGUAGE plpgsql` keyword anywhere
/// in the statement. Dollar tags can be empty (`$$`) or named (`$body$`).
fn body_opener_line_offset(ddl_statement: &str) -> Option<usize> {
    // Walk char-by-char looking for the first dollar-tag. We only care about a
    // simple structural match — the SQL parser already validated the statement.
    let bytes = ddl_statement.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            // Check for valid dollar-tag: $[tag]$ where tag is [A-Za-z0-9_]*
            let mut j = i + 1;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'$' {
                // Found a dollar-tag at position i..=j. Count newlines in [0..i).
                return Some(ddl_statement[..i].bytes().filter(|&b| b == b'\n').count());
            }
        }
        i += 1;
    }
    None
}

/// Strip the argument list from a regprocedure-style functionid such as
/// `api.create_chat(bigint,nanoid)` -> `api.create_chat`.
fn strip_function_args(functionid: &str) -> &str {
    match functionid.split_once('(') {
        Some((name, _)) => name,
        None => functionid,
    }
}

/// Find the scanned SqlObject (function/procedure) whose qualified name matches
/// the regprocedure-style `functionid` returned by plpgsql_check.
pub fn find_source_object<'a>(
    objects: &'a [SqlObject],
    functionid: &str,
) -> Option<&'a SqlObject> {
    let bare = strip_function_args(functionid);
    objects.iter().find(|obj| {
        if !matches!(obj.object_type, ObjectType::Function | ObjectType::Procedure) {
            return false;
        }
        let obj_name = match &obj.qualified_name.schema {
            Some(schema) => format!("{}.{}", schema, obj.qualified_name.name),
            None => obj.qualified_name.name.clone(),
        };
        obj_name == bare
    })
}

/// Compute the source file (path, 1-based line) for a plpgsql_check result,
/// looking up the function in the scanned source objects.
pub fn resolve_source_location(
    objects: &[SqlObject],
    functionid: &str,
    lineno: Option<i32>,
) -> (Option<String>, Option<usize>) {
    match find_source_object(objects, functionid) {
        Some(obj) => (
            obj.source_file.as_ref().map(|p| p.to_string_lossy().to_string()),
            calculate_source_line(obj, lineno),
        ),
        None => (None, None),
    }
}

/// Calculate the source file line number from the function-relative line number
/// reported by plpgsql_check.
///
/// plpgsql_check's `lineno` is 1-indexed within `pg_proc.prosrc`. Because the
/// body's opening `$tag$` is followed by a newline that gets stored in prosrc,
/// prosrc line 1 is empty and prosrc line N corresponds to the source file line
/// `D + N - 1` where D is the source file line containing `AS $tag$`.
///
/// D = function.start_line + (line offset from CREATE to the opening `$tag$`).
fn calculate_source_line(function: &SqlObject, function_line: Option<i32>) -> Option<usize> {
    let (start, line) = match (function.start_line, function_line) {
        (Some(s), Some(l)) => (s, l as usize),
        _ => return None,
    };
    let body_offset = body_opener_line_offset(&function.ddl_statement).unwrap_or(0);
    Some(start + body_offset + line.saturating_sub(1))
}

/// Format and display plpgsql_check errors, sorted by severity (warnings first, then errors)
pub fn display_check_errors(errors: &[PlpgsqlCheckError]) {
    if errors.is_empty() {
        return;
    }
    
    println!("\n{}", "=== PL/pgSQL Check Results ===".bold().yellow());
    
    // Sort errors by level - warnings first, then errors. plpgsql_check emits
    // variants like "warning extra", so match on prefix.
    fn level_order(level: &str) -> u8 {
        if level.starts_with("warning") {
            0
        } else if level.starts_with("error") {
            1
        } else {
            2
        }
    }

    let mut sorted_errors = errors.to_vec();
    sorted_errors.sort_by(|a, b| {
        let level_a = a.check_result.level.as_deref().unwrap_or("error");
        let level_b = b.check_result.level.as_deref().unwrap_or("error");
        level_order(level_a).cmp(&level_order(level_b))
    });
    
    for error in &sorted_errors {
        let level_str = error.check_result.level.as_deref().unwrap_or("error");
        let level_colored = if level_str.starts_with("error") {
            format!("{}", level_str.red().bold())
        } else if level_str.starts_with("warning") {
            format!("{}", level_str.yellow().bold())
        } else {
            format!("{}", level_str.blue().bold())
        };
        
        // Format location
        let location = match (&error.source_file, error.source_line) {
            (Some(file), Some(line)) => format!("{}:{}", file, line),
            (Some(file), None) => file.clone(),
            _ => error.function_name.clone(),
        };
        
        println!("\n{} {} in {}", 
            level_colored,
            format!("[{}]", error.check_result.sqlstate.as_deref().unwrap_or("00000")).dimmed(),
            location.cyan()
        );
        
        // Display the main message
        if let Some(message) = &error.check_result.message {
            println!("  {}", message);
        }
        
        // Display detail if available
        if let Some(detail) = &error.check_result.detail {
            println!("  {}: {}", "Detail".dimmed(), detail);
        }
        
        // Display hint if available
        if let Some(hint) = &error.check_result.hint {
            println!("  {}: {}", "Hint".green().dimmed(), hint);
        }
        
        // Display context if available
        if let Some(context) = &error.check_result.context {
            println!("  {}: {}", "Context".dimmed(), context);
        }
    }
    
    // Count warnings and errors (match prefix to include "warning extra", etc.)
    let warnings = sorted_errors.iter()
        .filter(|e| e.check_result.level.as_deref().is_some_and(|l| l.starts_with("warning")))
        .count();
    let errors_count = sorted_errors.iter()
        .filter(|e| e.check_result.level.as_deref().is_some_and(|l| l.starts_with("error")))
        .count();
    
    // Display summary
    print!("\n{} ", sorted_errors.len().to_string().yellow().bold());
    if warnings > 0 && errors_count > 0 {
        print!("issues ({} warnings, {} errors) ", warnings, errors_count);
    } else if warnings > 0 {
        print!("warning{} ", if warnings == 1 { "" } else { "s" });
    } else if errors_count > 0 {
        print!("error{} ", if errors_count == 1 { "" } else { "s" });
    } else {
        print!("issue{} ", if sorted_errors.len() == 1 { "" } else { "s" });
    }
    println!("found by plpgsql_check");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::QualifiedIdent;
    use std::path::PathBuf;
    
    fn make_function(ddl: &str, start_line: usize) -> SqlObject {
        let mut function = SqlObject::new(
            ObjectType::Function,
            QualifiedIdent::new(Some("test".to_string()), "my_func".to_string()),
            ddl.to_string(),
            Default::default(),
            Some(PathBuf::from("test.sql")),
        );
        function.start_line = Some(start_line);
        function
    }

    #[test]
    fn test_body_opener_line_offset_single_line() {
        let ddl = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN END; $$";
        assert_eq!(body_opener_line_offset(ddl), Some(0));
    }

    #[test]
    fn test_body_opener_line_offset_multi_line_signature() {
        // Body opens on line 5 (0-indexed: 4) of the statement.
        let ddl = "CREATE FUNCTION f(\n    a integer,\n    b integer\n)\nRETURNS void LANGUAGE plpgsql AS $body$\nBEGIN\nEND;\n$body$";
        assert_eq!(body_opener_line_offset(ddl), Some(4));
    }

    #[test]
    fn test_body_opener_line_offset_no_dollar_quote() {
        let ddl = "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS 'SELECT 1'";
        assert_eq!(body_opener_line_offset(ddl), None);
    }

    #[test]
    fn test_calculate_source_line_single_line_header() {
        // CREATE on source line 10; body opens on the same line (offset 0).
        // prosrc line 1 is the empty line right after `$$`, which sits on source
        // line 10. prosrc line 5 -> source line 10 + 0 + (5 - 1) = 14.
        let function = make_function(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    PERFORM 1;\n    SELECT bad FROM t;\nEND;\n$$",
            10,
        );
        assert_eq!(calculate_source_line(&function, Some(1)), Some(10));
        assert_eq!(calculate_source_line(&function, Some(5)), Some(14));
    }

    #[test]
    fn test_calculate_source_line_multi_line_header() {
        // Signature spans lines 10..=14; `AS $body$` on source line 14.
        // prosrc line 3 -> source line 14 + (3 - 1) = 16.
        let ddl = "CREATE FUNCTION f(\n    a integer,\n    b integer\n)\nRETURNS void LANGUAGE plpgsql AS $body$\nBEGIN\n    SELECT bad FROM t;\nEND;\n$body$";
        let function = make_function(ddl, 10);
        // Body offset is 4 lines from CREATE. start=10, offset=4, lineno=3 -> 16.
        assert_eq!(calculate_source_line(&function, Some(3)), Some(16));
    }

    #[test]
    fn test_calculate_source_line_no_inputs() {
        let function = make_function("CREATE FUNCTION f() ...", 10);
        assert_eq!(calculate_source_line(&function, None), None);

        let mut function = function;
        function.start_line = None;
        assert_eq!(calculate_source_line(&function, Some(1)), None);
    }
}