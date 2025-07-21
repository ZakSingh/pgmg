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
        // Register cleanup handlers on first use
        register_cleanup_handlers();
        
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
        // Clean up test database when test environment is dropped
        let database_name = self.database_name.clone();
        let container_info = CONTAINER_INFO.clone();
        
        tokio::spawn(async move {
            let info_guard = container_info.lock().await;
            if let Some(info) = info_guard.as_ref() {
                    let admin_conn_string = format!(
                        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
                        info.host_port
                    );
                    
                    if let Ok((admin_client, admin_connection)) = 
                        tokio_postgres::connect(&admin_conn_string, NoTls).await {
                        
                        tokio::spawn(async move {
                            if let Err(e) = admin_connection.await {
                                eprintln!("Cleanup admin connection error: {}", e);
                            }
                        });
                        
                        // Drop the test database
                        let _ = admin_client
                            .execute(&format!("DROP DATABASE IF EXISTS {}", database_name), &[])
                            .await;
                    }
                }
        });
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
            // Note: testcontainers automatically cleans up containers when dropped
        
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

/// Stop the shared PostgreSQL container (called on test completion)
pub async fn cleanup_shared_container() {
    let mut container_guard = CONTAINER.lock().await;
    let mut info_guard = CONTAINER_INFO.lock().await;
    
    if let Some(container) = container_guard.take() {
        // The container will be stopped and removed when dropped
        drop(container);
        *info_guard = None;
        println!("Cleaned up shared PostgreSQL container");
    }
}

/// Clean up all test resources - call this at the end of test suites
/// This is automatically called by cleanup handlers but can be called explicitly
pub async fn cleanup_all() {
    cleanup_shared_container().await;
}

/// Register cleanup handlers to ensure containers are stopped
pub fn register_cleanup_handlers() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    
    INIT.call_once(|| {
        // Register cleanup for normal program termination
        let _ = std::panic::set_hook(Box::new(|_| {
            // Try to cleanup on panic (best effort)
            std::thread::spawn(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(cleanup_shared_container());
            }).join().ok();
        }));
        
        // Register cleanup for SIGINT/SIGTERM
        #[cfg(unix)]
        {
            use std::sync::atomic::{AtomicBool, Ordering};
            use std::sync::Arc;
            
            let running = Arc::new(AtomicBool::new(true));
            
            ctrlc::set_handler(move || {
                if running.load(Ordering::SeqCst) {
                    running.store(false, Ordering::SeqCst);
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(cleanup_shared_container());
                    std::process::exit(0);
                }
            }).expect("Error setting Ctrl-C handler");
        }
    });
}

// Re-export modules
pub mod assertions;
pub mod fixtures;
pub mod plan_output;

// Add UUID dependency for unique database names
use uuid;