use pgmg::sql::splitter::split_sql_file;

#[test]
fn test_complex_file_line_tracking() {
    let sql = r#"-- File header comment
-- Second line of comment

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Function to get user by email
CREATE OR REPLACE FUNCTION get_user_by_email(user_email VARCHAR)
RETURNS TABLE(id INT, email VARCHAR, created_at TIMESTAMP)
LANGUAGE sql
AS $$
    SELECT id, email, created_at
    FROM users
    WHERE email = user_email;
$$;

CREATE INDEX idx_users_email ON users(email);

-- Another multi-line comment
-- that spans multiple lines
-- before the next statement

CREATE VIEW active_users AS
SELECT * FROM users
WHERE created_at > NOW() - INTERVAL '30 days';"#;

    let statements = split_sql_file(sql).unwrap();
    
    assert_eq!(statements.len(), 4);
    
    // CREATE TABLE
    assert!(statements[0].sql.contains("CREATE TABLE users"));
    assert_eq!(statements[0].start_line, Some(4));
    assert_eq!(statements[0].end_line, Some(8));
    
    // CREATE FUNCTION
    assert!(statements[1].sql.contains("CREATE OR REPLACE FUNCTION"));
    assert_eq!(statements[1].start_line, Some(11));
    assert_eq!(statements[1].end_line, Some(19));
    
    // CREATE INDEX
    assert!(statements[2].sql.contains("CREATE INDEX"));
    assert_eq!(statements[2].start_line, Some(21));
    assert_eq!(statements[2].end_line, Some(21));
    
    // CREATE VIEW
    assert!(statements[3].sql.contains("CREATE VIEW"));
    assert_eq!(statements[3].start_line, Some(27));
    assert_eq!(statements[3].end_line, Some(29));
}

#[test]
fn test_single_line_statements() {
    let sql = r#"CREATE TABLE t1 (id INT);
CREATE TABLE t2 (id INT);
CREATE TABLE t3 (id INT);"#;

    let statements = split_sql_file(sql).unwrap();
    
    assert_eq!(statements.len(), 3);
    
    assert_eq!(statements[0].start_line, Some(1));
    assert_eq!(statements[0].end_line, Some(1));
    
    assert_eq!(statements[1].start_line, Some(2));
    assert_eq!(statements[1].end_line, Some(2));
    
    assert_eq!(statements[2].start_line, Some(3));
    assert_eq!(statements[2].end_line, Some(3));
}