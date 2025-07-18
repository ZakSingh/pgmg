
#[derive(Debug, Clone)]
pub struct SqlStatement {
    pub index: usize,
    pub sql: String,
    pub start_location: Option<usize>,
}

/// Split SQL file content into individual statements using pg_query's parser
pub fn split_sql_file(file_content: &str) -> Result<Vec<SqlStatement>, Box<dyn std::error::Error>> {
    let statements = pg_query::split_with_parser(file_content)?;
    
    Ok(statements.into_iter()
        .enumerate()
        .map(|(index, stmt)| {
            SqlStatement {
                index,
                sql: stmt.trim().to_string(),
                start_location: None, // pg_query may provide this in the future
            }
        })
        .filter(|stmt| !stmt.sql.is_empty())
        .collect())
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