use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use testcontainers::{clients::Cli, Container, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

/// Container info stored globally
struct ContainerInfo {
    host_port: u16,
}

/// Shared PostgreSQL container for all tests
static DOCKER_CLIENT: Lazy<Cli> = Lazy::new(Cli::default);
static CONTAINER: Lazy<Arc<Mutex<Option<Container<'static, Postgres>>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(None)));
static CONTAINER_INFO: Lazy<Arc<Mutex<Option<ContainerInfo>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Test environment with isolated database and temporary directories
pub struct TestEnvironment {
    pub database_name: String,
    pub connection_string: String,
    pub temp_dir: TempDir,
    pub migrations_dir: PathBuf,
    pub sql_dir: PathBuf,
    pub client: Client,
}

impl TestEnvironment {
    /// Create a new test environment with isolated database
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Ensure container is started
        let container_info = ensure_container_started().await?;
        
        // Generate unique database name
        let database_name = format!("test_db_{}", uuid::Uuid::new_v4().to_string().replace("-", ""));
        
        // Connect to postgres database to create test database
        let host_port = container_info.host_port;
        let admin_conn_string = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/postgres",
            host_port
        );
        
        let (admin_client, admin_connection) = 
            tokio_postgres::connect(&admin_conn_string, NoTls).await?;
        
        tokio::spawn(async move {
            if let Err(e) = admin_connection.await {
                eprintln!("Admin connection error: {}", e);
            }
        });
        
        // Create test database
        admin_client
            .execute(&format!("CREATE DATABASE {}", database_name), &[])
            .await?;
        
        // Connect to test database
        let connection_string = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/{}",
            host_port, database_name
        );
        
        let (client, connection) = 
            tokio_postgres::connect(&connection_string, NoTls).await?;
        
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Test database connection error: {}", e);
            }
        });
        
        // Create temporary directories
        let temp_dir = TempDir::new()?;
        let migrations_dir = temp_dir.path().join("migrations");
        let sql_dir = temp_dir.path().join("sql");
        
        std::fs::create_dir(&migrations_dir)?;
        std::fs::create_dir(&sql_dir)?;
        
        Ok(Self {
            database_name,
            connection_string,
            temp_dir,
            migrations_dir,
            sql_dir,
            client,
        })
    }
    
    /// Write a migration file to the test environment
    pub async fn write_migration(
        &self,
        name: &str,
        content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_path = self.migrations_dir.join(format!("{}.sql", name));
        std::fs::write(file_path, content)?;
        Ok(())
    }
    
    /// Write a SQL object file to the test environment
    pub async fn write_sql_file(
        &self,
        name: &str,
        content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_path = self.sql_dir.join(name);
        std::fs::write(file_path, content)?;
        Ok(())
    }
    
    /// Execute raw SQL on the test database
    pub async fn execute_sql(&self, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.client.batch_execute(sql).await?;
        Ok(())
    }
    
    /// Check if a table exists in the test database
    pub async fn table_exists(&self, table_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let row = self.client
            .query_one(
                "SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )",
                &[&table_name],
            )
            .await?;
        Ok(row.get(0))
    }
    
    /// Check if a view exists in the test database
    pub async fn view_exists(&self, view_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let row = self.client
            .query_one(
                "SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )",
                &[&view_name],
            )
            .await?;
        Ok(row.get(0))
    }
    
    /// Check if a function exists in the test database
    pub async fn function_exists(&self, function_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let row = self.client
            .query_one(
                "SELECT EXISTS (
                    SELECT FROM information_schema.routines 
                    WHERE routine_schema = 'public' 
                    AND routine_name = $1
                )",
                &[&function_name],
            )
            .await?;
        Ok(row.get(0))
    }
    
    /// Get applied migrations from pgmg_migrations table
    pub async fn get_applied_migrations(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let rows = self.client
            .query(
                "SELECT name FROM pgmg_migrations ORDER BY name",
                &[],
            )
            .await?;
        
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }
    
    /// Get tracked objects from pgmg_state table
    pub async fn get_tracked_objects(&self) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
        let rows = self.client
            .query(
                "SELECT object_type, object_name FROM pgmg_state ORDER BY object_name",
                &[],
            )
            .await?;
        
        Ok(rows.into_iter().map(|row| (row.get(0), row.get(1))).collect())
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        // Drop test database when test environment is dropped
        // This happens automatically when TempDir is dropped
        // The database cleanup is handled by container restart between test runs
    }
}

/// Ensure the shared PostgreSQL container is started
async fn ensure_container_started() -> Result<ContainerInfo, Box<dyn std::error::Error>> {
    let mut container_guard = CONTAINER.lock().await;
    let mut info_guard = CONTAINER_INFO.lock().await;
    
    if container_guard.is_none() {
        let postgres_image = RunnableImage::from(Postgres::default())
            .with_env_var(("POSTGRES_PASSWORD", "postgres"))
            .with_env_var(("POSTGRES_USER", "postgres"))
            .with_env_var(("POSTGRES_DB", "postgres"));
        
        let container = DOCKER_CLIENT.run(postgres_image);
        let host_port = container.get_host_port_ipv4(5432);
        
        // Wait for PostgreSQL to be ready
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        *info_guard = Some(ContainerInfo { host_port });
        *container_guard = Some(container);
    }
    
    Ok(info_guard.as_ref().unwrap().clone())
}

impl Clone for ContainerInfo {
    fn clone(&self) -> Self {
        ContainerInfo {
            host_port: self.host_port,
        }
    }
}

// Re-export modules
pub mod assertions;
pub mod fixtures;

// Add UUID dependency for unique database names
use uuid;