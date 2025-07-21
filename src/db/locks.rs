use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tokio_postgres::Client;
use tracing::{debug, info, warn};

/// Advisory lock manager for pgmg operations
pub struct AdvisoryLockManager {
    lock_key: i64,
    is_locked: bool,
}

impl AdvisoryLockManager {
    /// Create a new advisory lock manager with a key derived from the connection string
    pub fn new(connection_string: &str) -> Self {
        let lock_key = generate_lock_key(connection_string);
        debug!("Generated advisory lock key: {}", lock_key);
        
        Self {
            lock_key,
            is_locked: false,
        }
    }

    /// Attempt to acquire the advisory lock with timeout and retry logic
    pub async fn acquire_lock(&mut self, client: &Client, timeout: Duration) -> Result<(), AdvisoryLockError> {
        if self.is_locked {
            return Err(AdvisoryLockError::AlreadyLocked);
        }

        let start_time = Instant::now();
        let retry_interval = Duration::from_secs(1);
        
        info!("Attempting to acquire advisory lock for pgmg apply operation...");
        
        loop {
            // Try to acquire lock non-blocking
            let acquired = self.try_acquire_lock_once(client).await?;
            
            if acquired {
                self.is_locked = true;
                info!("Successfully acquired advisory lock");
                return Ok(());
            }

            // Check if we've exceeded the timeout
            if start_time.elapsed() >= timeout {
                return Err(AdvisoryLockError::Timeout {
                    timeout_seconds: timeout.as_secs(),
                });
            }

            // Wait before retrying
            warn!("Advisory lock is held by another process, retrying in {}s...", retry_interval.as_secs());
            tokio::time::sleep(retry_interval).await;
        }
    }

    /// Try to acquire the lock once (non-blocking)
    async fn try_acquire_lock_once(&self, client: &Client) -> Result<bool, AdvisoryLockError> {
        let result = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&self.lock_key])
            .await
            .map_err(|e| AdvisoryLockError::DatabaseError(e.to_string()))?;
        
        let acquired: bool = result.get(0);
        debug!("Lock acquisition attempt result: {}", acquired);
        Ok(acquired)
    }

    /// Release the advisory lock
    pub async fn release_lock(&mut self, client: &Client) -> Result<(), AdvisoryLockError> {
        if !self.is_locked {
            debug!("Lock not currently held, nothing to release");
            return Ok(());
        }

        let result = client
            .query_one("SELECT pg_advisory_unlock($1)", &[&self.lock_key])
            .await
            .map_err(|e| AdvisoryLockError::DatabaseError(e.to_string()))?;
        
        let released: bool = result.get(0);
        
        if released {
            self.is_locked = false;
            info!("Successfully released advisory lock");
            Ok(())
        } else {
            warn!("Failed to release advisory lock - it may not have been held by this session");
            self.is_locked = false; // Reset state anyway
            Err(AdvisoryLockError::ReleaseFailed)
        }
    }

    /// Check if we currently hold the lock
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }

    /// Get the lock key for debugging purposes
    pub fn lock_key(&self) -> i64 {
        self.lock_key
    }
}

impl Drop for AdvisoryLockManager {
    fn drop(&mut self) {
        if self.is_locked {
            warn!("Advisory lock manager dropped while holding lock - this may leave the lock held until session ends");
            // Note: We can't call async methods in Drop, so we rely on PostgreSQL's
            // session cleanup to release the lock when the connection closes
        }
    }
}

/// Generate a consistent lock key from the connection string
fn generate_lock_key(connection_string: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    
    // Hash the connection string components that identify the database
    // but exclude credentials and other connection parameters
    let normalized = normalize_connection_string(connection_string);
    normalized.hash(&mut hasher);
    "pgmg_apply".hash(&mut hasher);
    
    // Convert to i64 for PostgreSQL advisory lock
    hasher.finish() as i64
}

/// Normalize connection string for consistent lock key generation
fn normalize_connection_string(conn_str: &str) -> String {
    // Extract just the host, port, and database name for lock key generation
    // This ensures the same database gets the same lock key regardless of
    // user credentials or other connection parameters
    
    if let Ok(url) = url::Url::parse(conn_str) {
        let host = url.host_str().unwrap_or("localhost");
        let port = url.port().unwrap_or(5432);
        let database = url.path().trim_start_matches('/');
        
        format!("postgres://{}:{}/{}", host, port, database)
    } else {
        // Fallback for non-URL connection strings
        conn_str.to_string()
    }
}

/// Errors that can occur during advisory lock operations
#[derive(Debug, thiserror::Error)]
pub enum AdvisoryLockError {
    #[error("Lock is already held by this session")]
    AlreadyLocked,
    
    #[error("Failed to acquire lock within {timeout_seconds} seconds - another pgmg apply process may be running")]
    Timeout { timeout_seconds: u64 },
    
    #[error("Failed to release advisory lock")]
    ReleaseFailed,
    
    #[error("Database error during lock operation: {0}")]
    DatabaseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_key_generation() {
        let conn1 = "postgresql://user:pass@localhost:5432/mydb";
        let conn2 = "postgresql://otheruser:otherpass@localhost:5432/mydb";
        let conn3 = "postgresql://user:pass@localhost:5432/otherdb";
        
        let key1 = generate_lock_key(conn1);
        let key2 = generate_lock_key(conn2);
        let key3 = generate_lock_key(conn3);
        
        // Same database should generate same key regardless of credentials
        assert_eq!(key1, key2);
        
        // Different database should generate different key
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_connection_string_normalization() {
        let conn1 = "postgresql://user:pass@localhost:5432/mydb?sslmode=require";
        let conn2 = "postgresql://otheruser:otherpass@localhost:5432/mydb";
        
        let norm1 = normalize_connection_string(conn1);
        let norm2 = normalize_connection_string(conn2);
        
        assert_eq!(norm1, "postgres://localhost:5432/mydb");
        assert_eq!(norm2, "postgres://localhost:5432/mydb");
        assert_eq!(norm1, norm2);
    }
}