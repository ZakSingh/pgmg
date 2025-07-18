use indoc::indoc;

/// Common SQL fixtures for testing
pub mod sql {
    use super::*;
    
    /// Simple users table
    pub const CREATE_USERS_TABLE: &str = indoc! {r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#};
    
    /// Posts table with foreign key to users
    pub const CREATE_POSTS_TABLE: &str = indoc! {r#"
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            title VARCHAR(255) NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#};
    
    /// Comments table with foreign keys
    pub const CREATE_COMMENTS_TABLE: &str = indoc! {r#"
        CREATE TABLE comments (
            id SERIAL PRIMARY KEY,
            post_id INTEGER NOT NULL REFERENCES posts(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#};
    
    /// View for recent posts
    pub const CREATE_RECENT_POSTS_VIEW: &str = indoc! {r#"
        CREATE VIEW recent_posts AS
        SELECT 
            p.id,
            p.title,
            p.content,
            u.username as author,
            p.created_at
        FROM posts p
        JOIN users u ON p.user_id = u.id
        WHERE p.created_at > CURRENT_DATE - INTERVAL '7 days'
        ORDER BY p.created_at DESC;
    "#};
    
    /// View for user statistics
    pub const CREATE_USER_STATS_VIEW: &str = indoc! {r#"
        CREATE VIEW user_stats AS
        SELECT 
            u.id as user_id,
            u.username,
            COUNT(DISTINCT p.id) as post_count
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id
        GROUP BY u.id, u.username;
    "#};
    
    /// Function to get user activity
    pub fn create_user_activity_function() -> &'static str {
        indoc! {r#"
            CREATE OR REPLACE FUNCTION get_user_activity(p_user_id INTEGER)
            RETURNS TABLE(
                post_count BIGINT,
                comment_count BIGINT,
                last_post_date TIMESTAMP,
                last_comment_date TIMESTAMP
            )
            AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    COUNT(DISTINCT p.id) as post_count,
                    COUNT(DISTINCT c.id) as comment_count,
                    MAX(p.created_at) as last_post_date,
                    MAX(c.created_at) as last_comment_date
                FROM users u
                LEFT JOIN posts p ON u.id = p.user_id
                LEFT JOIN comments c ON u.id = c.user_id
                WHERE u.id = p_user_id
                GROUP BY u.id;
            END;
            $$ LANGUAGE plpgsql;
        "#}
    }
    
    /// Type for user role
    pub const CREATE_USER_ROLE_TYPE: &str = indoc! {r#"
        CREATE TYPE user_role AS ENUM ('admin', 'moderator', 'user', 'guest');
    "#};
    
    /// Domain for email validation
    pub const CREATE_EMAIL_DOMAIN: &str = indoc! {r#"
        CREATE DOMAIN email_address AS VARCHAR(255)
        CHECK (VALUE ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$');
    "#};
    
    /// Index on posts for performance
    pub const CREATE_POSTS_USER_INDEX: &str = indoc! {r#"
        CREATE INDEX idx_posts_user_id ON posts(user_id);
    "#};
    
    /// Trigger for updated_at timestamp
    pub const CREATE_UPDATED_AT_TRIGGER: &str = indoc! {r#"
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        CREATE TRIGGER update_users_updated_at BEFORE UPDATE
        ON users FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    "#};
}

/// Migration fixtures
pub mod migrations {
    use super::*;
    
    /// Initial schema migration
    pub const INITIAL_SCHEMA: &str = indoc! {r#"
        -- Initial schema setup
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        INSERT INTO schema_version (version) VALUES (1);
    "#};
    
    /// Add users table migration
    pub const ADD_USERS_TABLE: &str = indoc! {r#"
        -- Add users table
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL UNIQUE,
            email VARCHAR(255) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_users_email ON users(email);
        CREATE INDEX idx_users_username ON users(username);
    "#};
    
    /// Add posts table migration
    pub const ADD_POSTS_TABLE: &str = indoc! {r#"
        -- Add posts table
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            title VARCHAR(255) NOT NULL,
            slug VARCHAR(255) NOT NULL UNIQUE,
            content TEXT,
            published BOOLEAN DEFAULT FALSE,
            published_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        
        CREATE INDEX idx_posts_user_id ON posts(user_id);
        CREATE INDEX idx_posts_published ON posts(published) WHERE published = TRUE;
        CREATE INDEX idx_posts_slug ON posts(slug);
    "#};
    
    /// Migration with error for testing rollback
    pub const MIGRATION_WITH_ERROR: &str = indoc! {r#"
        -- This migration will fail
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        
        -- This will cause an error
        INSERT INTO non_existent_table (id) VALUES (1);
    "#};
}

/// Helper to create parameterized SQL
pub mod builders {
    /// Create a simple table with given name
    pub fn create_table(name: &str) -> String {
        format!(
            "CREATE TABLE {} (\n    id SERIAL PRIMARY KEY,\n    name TEXT NOT NULL\n);",
            name
        )
    }
    
    /// Create a view that selects from a table
    pub fn create_simple_view(view_name: &str, table_name: &str) -> String {
        format!(
            "CREATE VIEW {} AS\nSELECT * FROM {};",
            view_name, table_name
        )
    }
    
    /// Create a function with a given name
    pub fn create_simple_function(name: &str) -> String {
        format!(
            r#"CREATE OR REPLACE FUNCTION {}()
RETURNS INTEGER AS $$
BEGIN
    RETURN 42;
END;
$$ LANGUAGE plpgsql;"#,
            name
        )
    }
}