use std::time::{SystemTime, UNIX_EPOCH};
use std::path::PathBuf;
use tokio_postgres::NoTls;
use url::Url;
use sha2::{Sha256, Digest};
use std::fs;
use crate::config::PgmgConfig;
use owo_colors::OwoColorize;

/// Parse a PostgreSQL connection string and extract its components
pub fn parse_connection_string(conn_str: &str) -> Result<ConnectionComponents, Box<dyn std::error::Error>> {
    let url = Url::parse(conn_str)?;
    
    let host = url.host_str().unwrap_or("localhost").to_string();
    let port = url.port().unwrap_or(5432);
    let database = url.path().trim_start_matches('/').to_string();
    let user = url.username().to_string();
    let password = url.password().map(|p| p.to_string());
    
    Ok(ConnectionComponents {
        host,
        port,
        database,
        user,
        password,
    })
}

#[derive(Debug, Clone)]
pub struct ConnectionComponents {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
}

/// Generate a unique test database name
pub fn generate_test_database_name(base_name: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}_test_{}", base_name, timestamp)
}

/// Build a connection string from components
pub fn build_connection_string(components: &ConnectionComponents, database: &str) -> String {
    if let Some(password) = &components.password {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            components.user, password, components.host, components.port, database
        )
    } else {
        format!(
            "postgresql://{}@{}:{}/{}",
            components.user, components.host, components.port, database
        )
    }
}

/// Create a test database
pub async fn create_test_database(
    admin_conn_str: &str,
    test_db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(admin_conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Admin connection error: {}", e);
        }
    });
    
    // Create the test database
    client
        .execute(&format!("CREATE DATABASE \"{}\"", test_db_name), &[])
        .await?;
    
    Ok(())
}

/// Drop a test database
pub async fn drop_test_database(
    admin_conn_str: &str,
    test_db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(admin_conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Admin connection error during cleanup: {}", e);
        }
    });
    
    // Force disconnect all connections to the test database
    let _ = client
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
            &[&test_db_name],
        )
        .await;
    
    // Drop the test database
    client
        .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", test_db_name), &[])
        .await?;
    
    Ok(())
}

/// Test database manager that ensures cleanup on drop
pub struct TestDatabase {
    pub name: String,
    pub connection_string: String,
    admin_connection_string: String,
    used_template: bool,
}

impl TestDatabase {
    pub async fn new(original_conn_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let components = parse_connection_string(original_conn_str)?;
        let test_db_name = generate_test_database_name(&components.database);
        
        // Admin connection uses 'postgres' database
        let admin_conn_str = build_connection_string(&components, "postgres");
        
        // Create the test database
        create_test_database(&admin_conn_str, &test_db_name).await?;
        
        // Build connection string for the new test database
        let test_conn_str = build_connection_string(&components, &test_db_name);
        
        Ok(TestDatabase {
            name: test_db_name,
            connection_string: test_conn_str,
            admin_connection_string: admin_conn_str,
            used_template: false,
        })
    }
    
    pub async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        drop_test_database(&self.admin_connection_string, &self.name).await
    }
    
    /// Create a test database using a template for faster setup
    pub async fn new_with_template(
        original_conn_str: &str,
        migrations_dir: Option<PathBuf>,
        code_dir: Option<PathBuf>,
        config: &PgmgConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let components = parse_connection_string(original_conn_str)?;
        let template_name = format!("{}_pgmg_template", components.database);
        let test_db_name = generate_test_database_name(&components.database);
        
        // Admin connection uses 'postgres' database
        let admin_conn_str = build_connection_string(&components, "postgres");
        
        // Check if template exists and is current
        let template_checksum = calculate_template_checksum(&migrations_dir, &code_dir)?;
        
        if !template_exists_and_current(&admin_conn_str, &template_name, &template_checksum).await? {
            println!("  {} Creating or updating template database...", "→".cyan());
            create_template_database(
                &admin_conn_str,
                &template_name,
                &components,
                migrations_dir,
                code_dir,
                config,
                &template_checksum,
            ).await?;
            println!("  {} Template database ready", "✓".green());
        }
        
        // Clone from template
        clone_from_template(&admin_conn_str, &template_name, &test_db_name).await?;
        
        // Build connection string for the new test database
        let test_conn_str = build_connection_string(&components, &test_db_name);
        
        Ok(TestDatabase {
            name: test_db_name,
            connection_string: test_conn_str,
            admin_connection_string: admin_conn_str,
            used_template: true,
        })
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Schedule cleanup in a detached task
        let admin_conn = self.admin_connection_string.clone();
        let db_name = self.name.clone();
        
        tokio::spawn(async move {
            if let Err(e) = drop_test_database(&admin_conn, &db_name).await {
                eprintln!("Failed to drop test database '{}': {}", db_name, e);
            }
        });
    }
}

/// Calculate a checksum of all migration AND code files
fn calculate_template_checksum(
    migrations_dir: &Option<PathBuf>,
    code_dir: &Option<PathBuf>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut hasher = Sha256::new();

    // Hash migrations (non-recursive, just top-level .sql files)
    if let Some(dir) = migrations_dir {
        hash_sql_directory(&mut hasher, dir)?;
    }

    // Hash code directory (recursively)
    if let Some(dir) = code_dir {
        hash_sql_directory_recursive(&mut hasher, dir)?;
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Hash all .sql files in a directory (non-recursive)
fn hash_sql_directory(hasher: &mut Sha256, dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if !dir.exists() {
        return Ok(());
    }

    let mut entries: Vec<_> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "sql")
                .unwrap_or(false)
        })
        .collect();

    // Sort for consistent ordering
    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let content = fs::read_to_string(entry.path())?;
        hasher.update(content.as_bytes());
        hasher.update(b"\n");
    }

    Ok(())
}

/// Hash all .sql files in a directory recursively
fn hash_sql_directory_recursive(hasher: &mut Sha256, dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if !dir.exists() {
        return Ok(());
    }

    let mut paths: Vec<PathBuf> = Vec::new();
    collect_sql_files_recursive(dir, &mut paths)?;
    paths.sort(); // Consistent ordering

    for path in paths {
        let content = fs::read_to_string(&path)?;
        hasher.update(content.as_bytes());
        hasher.update(b"\n");
    }

    Ok(())
}

/// Recursively collect all .sql files from a directory
fn collect_sql_files_recursive(dir: &PathBuf, paths: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            collect_sql_files_recursive(&path, paths)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
            paths.push(path);
        }
    }
    Ok(())
}

/// Check if template database exists and has current migrations
async fn template_exists_and_current(
    admin_conn_str: &str,
    template_name: &str,
    expected_checksum: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(admin_conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Admin connection error: {}", e);
        }
    });
    
    // Check if database exists
    let exists = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&template_name],
        )
        .await?
        .get::<_, bool>(0);
    
    if !exists {
        return Ok(false);
    }
    
    // Check if migrations are current
    // Parse the admin connection string and rebuild it with the template database
    let admin_components = parse_connection_string(admin_conn_str)?;
    let template_conn_str = build_connection_string(&admin_components, template_name);
    let (template_client, template_connection) = tokio_postgres::connect(&template_conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = template_connection.await {
            eprintln!("Template connection error: {}", e);
        }
    });
    
    // Check for pgmg schema and template info table
    let has_info = template_client
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'pgmg' 
                AND table_name = 'template_info'
            )",
            &[],
        )
        .await?
        .get::<_, bool>(0);
    
    if !has_info {
        return Ok(false);
    }
    
    // Check checksum
    let rows = template_client
        .query(
            "SELECT migrations_checksum FROM pgmg.template_info ORDER BY created_at DESC LIMIT 1",
            &[],
        )
        .await?;
    
    if rows.is_empty() {
        return Ok(false);
    }
    
    let stored_checksum: String = rows[0].get(0);
    Ok(stored_checksum == expected_checksum)
}

/// Create a template database with all migrations applied
async fn create_template_database(
    admin_conn_str: &str,
    template_name: &str,
    components: &ConnectionComponents,
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    config: &PgmgConfig,
    migrations_checksum: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Drop existing template if it exists
    let _ = drop_test_database(admin_conn_str, template_name).await;
    
    // Create new template database
    create_test_database(admin_conn_str, template_name).await?;
    
    // Build connection string for template
    let template_conn_str = build_connection_string(components, template_name);
    
    // Apply migrations to template
    let apply_result = crate::commands::apply::execute_apply_with_test_mode(
        migrations_dir,
        code_dir,
        template_conn_str.clone(),
        config,
        true, // test_mode
    ).await?;
    
    if !apply_result.errors.is_empty() {
        // Clean up failed template
        let _ = drop_test_database(admin_conn_str, template_name).await;
        return Err(format!("Failed to apply migrations to template: {:?}", apply_result.errors).into());
    }
    
    // Store migrations checksum
    let (client, connection) = tokio_postgres::connect(&template_conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Template connection error: {}", e);
        }
    });
    
    // Create template info table
    client.execute(
        "CREATE SCHEMA IF NOT EXISTS pgmg",
        &[],
    ).await?;
    
    client.execute(
        "CREATE TABLE IF NOT EXISTS pgmg.template_info (
            migrations_checksum TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
        &[],
    ).await?;
    
    client.execute(
        "INSERT INTO pgmg.template_info (migrations_checksum) VALUES ($1)",
        &[&migrations_checksum],
    ).await?;
    
    Ok(())
}

/// Clone a database from template
async fn clone_from_template(
    admin_conn_str: &str,
    template_name: &str,
    new_db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(admin_conn_str, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Admin connection error: {}", e);
        }
    });
    
    // Clone from template - this is MUCH faster than running migrations
    client
        .execute(
            &format!("CREATE DATABASE \"{}\" WITH TEMPLATE \"{}\"", new_db_name, template_name),
            &[],
        )
        .await?;
    
    Ok(())
}