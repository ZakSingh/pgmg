
#[derive(Debug, Clone)]
pub struct SqlStatement {
    pub index: usize,
    pub sql: String,
    pub start_location: Option<usize>,
    pub start_line: Option<usize>,
    pub end_line: Option<usize>,
}

/// Split SQL file content into individual statements using pg_query's parser
pub fn split_sql_file(file_content: &str) -> Result<Vec<SqlStatement>, Box<dyn std::error::Error>> {
    let raw_statements = pg_query::split_with_parser(file_content)?;
    
    // Build a line offset map to convert character positions to line numbers
    let line_offsets = build_line_offset_map(file_content);
    
    let mut statements = Vec::new();
    let mut current_pos = 0;
    
    for (index, stmt) in raw_statements.into_iter().enumerate() {
        let trimmed_sql = stmt.trim();
        if trimmed_sql.is_empty() {
            continue;
        }
        
        // Find the statement in the original content to get accurate positions
        // Note: pg_query returns statements without leading/trailing whitespace
        if let Some(stmt_start) = file_content[current_pos..].find(trimmed_sql) {
            let absolute_start = current_pos + stmt_start;
            let absolute_end = absolute_start + trimmed_sql.len();
            
            // Convert character positions to line numbers
            let start_line = position_to_line(absolute_start, &line_offsets);
            let end_line = position_to_line(absolute_end.saturating_sub(1), &line_offsets);
            
            statements.push(SqlStatement {
                index,
                sql: trimmed_sql.to_string(),
                start_location: Some(absolute_start),
                start_line: Some(start_line),
                end_line: Some(end_line),
            });
            
            current_pos = absolute_end;
        } else {
            // Fallback if we can't find the statement
            statements.push(SqlStatement {
                index,
                sql: trimmed_sql.to_string(),
                start_location: None,
                start_line: None,
                end_line: None,
            });
        }
    }
    
    Ok(statements)
}

/// Build a map of line start positions for efficient line number lookup
fn build_line_offset_map(content: &str) -> Vec<usize> {
    let mut offsets = vec![0]; // First line starts at position 0
    
    for (pos, ch) in content.char_indices() {
        if ch == '\n' {
            offsets.push(pos + 1);
        }
    }
    
    offsets
}

/// Convert a character position to a line number (1-based)
fn position_to_line(pos: usize, line_offsets: &[usize]) -> usize {
    match line_offsets.binary_search(&pos) {
        Ok(line) => line + 1,
        Err(line) => line,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_single_statement() {
        let sql = "SELECT * FROM users;";
        let result = split_sql_file(sql).unwrap();
        
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].sql, "SELECT * FROM users");
        assert_eq!(result[0].index, 0);
        assert_eq!(result[0].start_line, Some(1));
        assert_eq!(result[0].end_line, Some(1));
    }

    #[test]
    fn test_split_multiple_statements() {
        let sql = r#"
            CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
            INSERT INTO users (name) VALUES ('Alice');
            SELECT * FROM users;
        "#;
        let result = split_sql_file(sql).unwrap();
        
        assert_eq!(result.len(), 3);
        assert!(result[0].sql.contains("CREATE TABLE"));
        assert!(result[1].sql.contains("INSERT"));
        assert!(result[2].sql.contains("SELECT"));
    }
    
    #[test]
    fn test_line_number_tracking() {
        let sql = r#"-- Comment on line 1
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT
);

-- Another comment
INSERT INTO users (name) 
VALUES ('Alice');

SELECT * FROM users;"#;
        
        let result = split_sql_file(sql).unwrap();
        
        assert_eq!(result.len(), 3);
        
        // CREATE TABLE statement includes the comment and spans lines 1-5
        assert_eq!(result[0].start_line, Some(1));
        assert_eq!(result[0].end_line, Some(5));
        
        // INSERT statement includes the comment and spans lines 7-9
        assert_eq!(result[1].start_line, Some(7));
        assert_eq!(result[1].end_line, Some(9));
        
        // SELECT statement is on line 11
        assert_eq!(result[2].start_line, Some(11));
        assert_eq!(result[2].end_line, Some(11));
    }

    #[test]
    fn test_split_with_empty_lines() {
        let sql = r#"
            
            CREATE TABLE test (id INT);
            
            
            INSERT INTO test VALUES (1);
            
        "#;
        let result = split_sql_file(sql).unwrap();
        
        assert_eq!(result.len(), 2);
        assert!(result[0].sql.contains("CREATE TABLE"));
        assert!(result[1].sql.contains("INSERT"));
    }

    #[test]
    fn test_split_function_with_dollar_quotes() {
        let sql = r#"
            CREATE OR REPLACE FUNCTION test_func() RETURNS void AS $$
            BEGIN
                INSERT INTO test_table VALUES (1, 'test');
            END;
            $$ LANGUAGE plpgsql;
            
            SELECT test_func();
        "#;
        let result = split_sql_file(sql).unwrap();
        
        assert_eq!(result.len(), 2);
        assert!(result[0].sql.contains("CREATE OR REPLACE FUNCTION"));
        assert!(result[1].sql.contains("SELECT test_func"));
    }
}