use tokio_postgres::Client;
use std::collections::HashSet;
use crate::sql::{ObjectType, QualifiedIdent};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct MigrationRecord {
    pub name: String,
    pub applied_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ObjectRecord {
    pub object_type: ObjectType,
    pub object_name: QualifiedIdent,
    pub ddl_hash: String,
    pub last_applied: SystemTime,
}

pub struct StateManager<'a> {
    client: &'a Client,
}

impl<'a> StateManager<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Initialize the state tracking tables if they don't exist
    pub async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create pgmg_migrations table
        self.client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgmg_migrations (
                name TEXT PRIMARY KEY,
                applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
            "#,
            &[],
        ).await?;

        // Create pgmg_state table for object tracking
        self.client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgmg_state (
                object_type TEXT NOT NULL,
                object_name TEXT NOT NULL,
                ddl_hash TEXT NOT NULL,
                last_applied TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                PRIMARY KEY (object_type, object_name)
            )
            "#,
            &[],
        ).await?;

        Ok(())
    }

    /// Get all applied migrations
    pub async fn get_applied_migrations(&self) -> Result<Vec<MigrationRecord>, Box<dyn std::error::Error>> {
        let rows = self.client.query(
            "SELECT name, applied_at FROM pgmg_migrations ORDER BY applied_at",
            &[],
        ).await?;

        let mut migrations = Vec::new();
        for row in rows {
            migrations.push(MigrationRecord {
                name: row.get(0),
                applied_at: row.get(1),
            });
        }

        Ok(migrations)
    }

    /// Record a migration as applied
    pub async fn record_migration(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.client.execute(
            "INSERT INTO pgmg_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[&name],
        ).await?;

        Ok(())
    }

    /// Get all tracked objects with their current hashes
    pub async fn get_tracked_objects(&self) -> Result<Vec<ObjectRecord>, Box<dyn std::error::Error>> {
        let rows = self.client.query(
            "SELECT object_type, object_name, ddl_hash, last_applied FROM pgmg_state ORDER BY object_name",
            &[],
        ).await?;

        let mut objects = Vec::new();
        for row in rows {
            let object_type_str: String = row.get(0);
            let object_type = match object_type_str.as_str() {
                "view" => ObjectType::View,
                "function" => ObjectType::Function,
                "type" => ObjectType::Type,
                "domain" => ObjectType::Domain,
                "index" => ObjectType::Index,
                "trigger" => ObjectType::Trigger,
                _ => continue, // Skip unknown types
            };

            let object_name_str: String = row.get(1);
            let object_name = QualifiedIdent::from_qualified_name(&object_name_str);

            objects.push(ObjectRecord {
                object_type,
                object_name,
                ddl_hash: row.get(2),
                last_applied: row.get(3),
            });
        }

        Ok(objects)
    }

    /// Update or insert an object's hash
    pub async fn update_object_hash(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
        ddl_hash: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let object_type_str = match object_type {
            ObjectType::Table => "table",
            ObjectType::View => "view",
            ObjectType::Function => "function",
            ObjectType::Type => "type",
            ObjectType::Domain => "domain",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
        };

        let qualified_name = match &object_name.schema {
            Some(schema) => format!("{}.{}", schema, object_name.name),
            None => object_name.name.clone(),
        };

        self.client.execute(
            r#"
            INSERT INTO pgmg_state (object_type, object_name, ddl_hash) 
            VALUES ($1, $2, $3)
            ON CONFLICT (object_type, object_name) 
            DO UPDATE SET ddl_hash = $3, last_applied = NOW()
            "#,
            &[&object_type_str, &qualified_name, &ddl_hash],
        ).await?;

        Ok(())
    }

    /// Remove an object from tracking (when it's deleted)
    pub async fn remove_object(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let object_type_str = match object_type {
            ObjectType::Table => "table",
            ObjectType::View => "view",
            ObjectType::Function => "function",
            ObjectType::Type => "type",
            ObjectType::Domain => "domain",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
        };

        let qualified_name = match &object_name.schema {
            Some(schema) => format!("{}.{}", schema, object_name.name),
            None => object_name.name.clone(),
        };

        self.client.execute(
            "DELETE FROM pgmg_state WHERE object_type = $1 AND object_name = $2",
            &[&object_type_str, &qualified_name],
        ).await?;

        Ok(())
    }

    /// Get the hash for a specific object, if it exists
    pub async fn get_object_hash(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let object_type_str = match object_type {
            ObjectType::Table => "table",
            ObjectType::View => "view",
            ObjectType::Function => "function",
            ObjectType::Type => "type",
            ObjectType::Domain => "domain",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
        };

        let qualified_name = match &object_name.schema {
            Some(schema) => format!("{}.{}", schema, object_name.name),
            None => object_name.name.clone(),
        };

        let rows = self.client.query(
            "SELECT ddl_hash FROM pgmg_state WHERE object_type = $1 AND object_name = $2",
            &[&object_type_str, &qualified_name],
        ).await?;

        if let Some(row) = rows.first() {
            Ok(Some(row.get(0)))
        } else {
            Ok(None)
        }
    }

    /// Get names of all applied migrations
    pub async fn get_applied_migration_names(&self) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
        let rows = self.client.query(
            "SELECT name FROM pgmg_migrations",
            &[],
        ).await?;

        let mut names = HashSet::new();
        for row in rows {
            names.insert(row.get(0));
        }

        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: These tests would require a real database connection
    // For now, we'll add integration tests that can be run manually
    
    #[test]
    fn test_object_type_string_conversion() {
        let view_type = ObjectType::View;
        let type_str = match view_type {
            ObjectType::Table => "table",
            ObjectType::View => "view",
            ObjectType::Function => "function",
            ObjectType::Type => "type",
            ObjectType::Domain => "domain",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
        };
        
        assert_eq!(type_str, "view");
    }

    #[test]
    fn test_qualified_name_formatting() {
        let qualified = QualifiedIdent::new(Some("api".to_string()), "user_stats".to_string());
        let formatted = match &qualified.schema {
            Some(schema) => format!("{}.{}", schema, qualified.name),
            None => qualified.name.clone(),
        };
        
        assert_eq!(formatted, "api.user_stats");
        
        let unqualified = QualifiedIdent::from_name("users".to_string());
        let formatted2 = match &unqualified.schema {
            Some(schema) => format!("{}.{}", schema, unqualified.name),
            None => unqualified.name.clone(),
        };
        
        assert_eq!(formatted2, "users");
    }
}