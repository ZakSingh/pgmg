use tokio_postgres::Client;
use std::collections::HashSet;
use crate::sql::{ObjectType, QualifiedIdent, state_object_name, parse_state_object_name};
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
    /// For triggers only: the table the trigger is defined on (parsed from the
    /// `name:table` state key). None for legacy-format trigger rows.
    pub trigger_table: Option<QualifiedIdent>,
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
        // Suppress NOTICE messages during initialization
        // (PostgreSQL emits "already exists, skipping" for IF NOT EXISTS)
        self.client.execute("SET client_min_messages = 'WARNING'", &[]).await?;

        // Create pgmg schema if it doesn't exist
        self.client.execute(
            r#"
            CREATE SCHEMA IF NOT EXISTS pgmg
            "#,
            &[],
        ).await?;

        // Create pgmg_migrations table
        self.client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgmg.pgmg_migrations (
                name TEXT PRIMARY KEY,
                applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
            "#,
            &[],
        ).await?;

        // Create pgmg_state table for object tracking
        self.client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgmg.pgmg_state (
                object_type TEXT NOT NULL,
                object_name TEXT NOT NULL,
                ddl_hash TEXT NOT NULL,
                last_applied TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                PRIMARY KEY (object_type, object_name)
            )
            "#,
            &[],
        ).await?;

        // Create pgmg_dependencies table for tracking object dependencies
        self.client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pgmg.pgmg_dependencies (
                dependent_type TEXT NOT NULL,
                dependent_name TEXT NOT NULL,
                dependency_type TEXT NOT NULL,
                dependency_name TEXT NOT NULL,
                dependency_kind TEXT NOT NULL,
                PRIMARY KEY (dependent_type, dependent_name, dependency_type, dependency_name)
            )
            "#,
            &[],
        ).await?;

        // Create indexes for performance optimization
        // Index on object_type for filtering queries by type
        self.client.execute(
            r#"
            CREATE INDEX IF NOT EXISTS idx_pgmg_state_object_type
            ON pgmg.pgmg_state (object_type)
            "#,
            &[],
        ).await?;

        // Index on last_applied for time-based queries
        self.client.execute(
            r#"
            CREATE INDEX IF NOT EXISTS idx_pgmg_state_last_applied
            ON pgmg.pgmg_state (last_applied)
            "#,
            &[],
        ).await?;

        // Index on migrations applied_at for chronological queries
        self.client.execute(
            r#"
            CREATE INDEX IF NOT EXISTS idx_pgmg_migrations_applied_at
            ON pgmg.pgmg_migrations (applied_at)
            "#,
            &[],
        ).await?;

        // Indexes for dependency lookups
        self.client.execute(
            r#"
            CREATE INDEX IF NOT EXISTS idx_pgmg_dependencies_dependent
            ON pgmg.pgmg_dependencies (dependent_type, dependent_name)
            "#,
            &[],
        ).await?;

        self.client.execute(
            r#"
            CREATE INDEX IF NOT EXISTS idx_pgmg_dependencies_dependency
            ON pgmg.pgmg_dependencies (dependency_type, dependency_name)
            "#,
            &[],
        ).await?;

        self.upgrade_legacy_trigger_identity().await?;

        // Restore default message level
        self.client.execute("SET client_min_messages = 'NOTICE'", &[]).await?;

        Ok(())
    }

    /// One-time upgrade of legacy trigger state keys from `name` to `name:table`.
    ///
    /// Trigger identity used to be the bare trigger name, which collides when
    /// two triggers share a name on different tables. Legacy rows (no ':' in
    /// the key) are expanded into one row per recorded relation dependency,
    /// carrying the legacy hash. A trigger with several relation deps (e.g. a
    /// constraint trigger, or a name shared by two triggers whose deps merged
    /// under the one key) gets one row per table — at worst a spurious row
    /// that resolves as a no-op `DROP TRIGGER IF EXISTS` or a cheap recreate
    /// on the next apply. Legacy rows with no relation dep are dropped; the
    /// trigger re-plans as a create, which pre-drops defensively.
    ///
    /// Idempotent: upgraded keys contain ':', so re-runs match nothing.
    /// Comment rows also contain ':' but have object_type = 'comment' and are
    /// never touched.
    async fn upgrade_legacy_trigger_identity(&self) -> Result<(), Box<dyn std::error::Error>> {
        // batch_execute runs all statements in one implicit transaction
        self.client.batch_execute(
            r#"
            -- 1. Expand each legacy trigger state row into one row per relation dep.
            INSERT INTO pgmg.pgmg_state (object_type, object_name, ddl_hash, last_applied)
            SELECT s.object_type,
                   s.object_name || ':' || d.dependency_name,
                   s.ddl_hash,
                   s.last_applied
            FROM pgmg.pgmg_state s
            JOIN pgmg.pgmg_dependencies d
              ON d.dependent_type = 'trigger'
             AND d.dependent_name = s.object_name
             AND d.dependency_type = 'relation'
            WHERE s.object_type = 'trigger'
              AND position(':' in s.object_name) = 0
            ON CONFLICT (object_type, object_name) DO NOTHING;

            -- 2. Drop all legacy trigger state rows.
            DELETE FROM pgmg.pgmg_state
            WHERE object_type = 'trigger'
              AND position(':' in object_name) = 0;

            -- 3. Rewrite legacy trigger dependency rows: each relation dep
            --    attaches to its own composite identity; function/type deps
            --    replicate to every expanded identity (safe over-approximation,
            --    replaced wholesale on the trigger's next recreate).
            INSERT INTO pgmg.pgmg_dependencies
              (dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind)
            SELECT d.dependent_type,
                   d.dependent_name || ':' || r.dependency_name,
                   d.dependency_type,
                   d.dependency_name,
                   d.dependency_kind
            FROM pgmg.pgmg_dependencies d
            JOIN pgmg.pgmg_dependencies r
              ON r.dependent_type = 'trigger'
             AND r.dependent_name = d.dependent_name
             AND r.dependency_type = 'relation'
            WHERE d.dependent_type = 'trigger'
              AND position(':' in d.dependent_name) = 0
              AND (d.dependency_type <> 'relation' OR d.dependency_name = r.dependency_name)
            ON CONFLICT DO NOTHING;

            -- 4. Drop all legacy trigger dependency rows.
            DELETE FROM pgmg.pgmg_dependencies
            WHERE dependent_type = 'trigger'
              AND position(':' in dependent_name) = 0;
            "#,
        ).await?;

        Ok(())
    }

    /// Get all applied migrations
    pub async fn get_applied_migrations(&self) -> Result<Vec<MigrationRecord>, Box<dyn std::error::Error>> {
        let rows = self.client.query(
            "SELECT name, applied_at FROM pgmg.pgmg_migrations ORDER BY applied_at",
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
            "INSERT INTO pgmg.pgmg_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[&name],
        ).await?;

        Ok(())
    }

    /// Get all tracked objects with their current hashes
    pub async fn get_tracked_objects(&self) -> Result<Vec<ObjectRecord>, Box<dyn std::error::Error>> {
        let rows = self.client.query(
            "SELECT object_type, object_name, ddl_hash, last_applied FROM pgmg.pgmg_state ORDER BY object_name",
            &[],
        ).await?;

        let mut objects = Vec::new();
        for row in rows {
            let object_type_str: String = row.get(0);
            let object_type = match self.string_to_object_type(&object_type_str) {
                Some(object_type) => object_type,
                None => continue, // Skip unknown types
            };

            let object_name_str: String = row.get(1);
            let (object_name, trigger_table) = parse_state_object_name(&object_type, &object_name_str);

            objects.push(ObjectRecord {
                object_type,
                object_name,
                ddl_hash: row.get(2),
                last_applied: row.get(3),
                trigger_table,
            });
        }

        Ok(objects)
    }

    /// Check if pgmg has never applied anything to this database (fresh build):
    /// no recorded migrations AND no tracked objects. Checking migrations alone
    /// would misclassify migration-less (declarative-only) projects as fresh on
    /// every apply, silently disabling transactional mode.
    pub async fn is_empty(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let count: i64 = self.client.query_one(
            "SELECT (SELECT COUNT(*) FROM pgmg.pgmg_migrations)
                  + (SELECT COUNT(*) FROM pgmg.pgmg_state)",
            &[],
        ).await?.get(0);
        Ok(count == 0)
    }

    /// Update or insert an object's hash
    pub async fn update_object_hash(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
        trigger_table: Option<&QualifiedIdent>,
        ddl_hash: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let object_type_str = self.object_type_to_string(object_type);
        let qualified_name = state_object_name(object_type, object_name, trigger_table);

        self.client.execute(
            r#"
            INSERT INTO pgmg.pgmg_state (object_type, object_name, ddl_hash) 
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
        trigger_table: Option<&QualifiedIdent>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let object_type_str = self.object_type_to_string(object_type);
        let qualified_name = state_object_name(object_type, object_name, trigger_table);

        self.client.execute(
            "DELETE FROM pgmg.pgmg_state WHERE object_type = $1 AND object_name = $2",
            &[&object_type_str, &qualified_name],
        ).await?;

        // Also remove dependencies
        self.remove_object_dependencies(object_type, object_name, trigger_table).await?;

        Ok(())
    }

    /// Get the hash for a specific object, if it exists
    pub async fn get_object_hash(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
        trigger_table: Option<&QualifiedIdent>,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let object_type_str = self.object_type_to_string(object_type);
        let qualified_name = state_object_name(object_type, object_name, trigger_table);

        let rows = self.client.query(
            "SELECT ddl_hash FROM pgmg.pgmg_state WHERE object_type = $1 AND object_name = $2",
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
            "SELECT name FROM pgmg.pgmg_migrations",
            &[],
        ).await?;

        let mut names = HashSet::new();
        for row in rows {
            names.insert(row.get(0));
        }

        Ok(names)
    }

    /// Store dependencies for an object
    pub async fn store_object_dependencies(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
        trigger_table: Option<&QualifiedIdent>,
        dependencies: &crate::sql::Dependencies,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let object_type_str = self.object_type_to_string(object_type);
        let qualified_name = state_object_name(object_type, object_name, trigger_table);

        // First, remove existing dependencies for this object
        self.client.execute(
            "DELETE FROM pgmg.pgmg_dependencies WHERE dependent_type = $1 AND dependent_name = $2",
            &[&object_type_str, &qualified_name],
        ).await?;
        
        // Store relation dependencies
        for dep in &dependencies.relations {
            let dep_qualified = self.format_qualified_name(dep);
            // Relations could be tables, views, or materialized views - we store as generic "relation"
            self.client.execute(
                r#"
                INSERT INTO pgmg.pgmg_dependencies 
                (dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind)
                VALUES ($1, $2, 'relation', $3, 'hard')
                "#,
                &[&object_type_str, &qualified_name, &dep_qualified],
            ).await?;
        }
        
        // Store function dependencies
        for dep in &dependencies.functions {
            let dep_qualified = self.format_qualified_name(dep);
            // Determine dependency kind based on dependent object type
            let dep_kind = match object_type {
                ObjectType::Function | ObjectType::Procedure => "soft",
                _ => "hard",
            };
            self.client.execute(
                r#"
                INSERT INTO pgmg.pgmg_dependencies 
                (dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind)
                VALUES ($1, $2, 'function', $3, $4)
                "#,
                &[&object_type_str, &qualified_name, &dep_qualified, &dep_kind],
            ).await?;
        }
        
        // Store type dependencies
        for dep in &dependencies.types {
            let dep_qualified = self.format_qualified_name(dep);
            self.client.execute(
                r#"
                INSERT INTO pgmg.pgmg_dependencies 
                (dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind)
                VALUES ($1, $2, 'type', $3, 'hard')
                "#,
                &[&object_type_str, &qualified_name, &dep_qualified],
            ).await?;
        }
        
        Ok(())
    }
    
    /// Remove all dependencies for an object (when it's deleted)
    pub async fn remove_object_dependencies(
        &self,
        object_type: &ObjectType,
        object_name: &QualifiedIdent,
        trigger_table: Option<&QualifiedIdent>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let object_type_str = self.object_type_to_string(object_type);
        let qualified_name = state_object_name(object_type, object_name, trigger_table);

        // Remove as dependent
        self.client.execute(
            "DELETE FROM pgmg.pgmg_dependencies WHERE dependent_type = $1 AND dependent_name = $2",
            &[&object_type_str, &qualified_name],
        ).await?;
        
        // Remove as dependency (cleanup references from other objects)
        self.client.execute(
            "DELETE FROM pgmg.pgmg_dependencies WHERE dependency_type = $1 AND dependency_name = $2",
            &[&object_type_str, &qualified_name],
        ).await?;
        
        Ok(())
    }
    
    /// Get stored dependencies for deleted objects.
    ///
    /// `deleted_objects` names are the raw state strings (`name:table` for
    /// triggers); the returned tuples carry the parsed name and trigger table.
    pub async fn get_deleted_object_dependencies(
        &self,
        deleted_objects: &[(ObjectType, String)],
    ) -> Result<Vec<(ObjectType, QualifiedIdent, Option<QualifiedIdent>, crate::sql::Dependencies)>, Box<dyn std::error::Error>> {
        if deleted_objects.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut result = Vec::new();
        
        for (object_type, object_name) in deleted_objects {
            let object_type_str = self.object_type_to_string(object_type);
            
            // Query dependencies for this deleted object
            let rows = self.client.query(
                r#"
                SELECT dependency_type, dependency_name, dependency_kind
                FROM pgmg.pgmg_dependencies
                WHERE dependent_type = $1 AND dependent_name = $2
                "#,
                &[&object_type_str, object_name],
            ).await?;
            
            let mut dependencies = crate::sql::Dependencies::default();
            
            for row in rows {
                let dep_type: String = row.get(0);
                let dep_name: String = row.get(1);
                let dep_qualified = QualifiedIdent::from_qualified_name(&dep_name);
                
                match dep_type.as_str() {
                    "relation" => {
                        dependencies.relations.insert(dep_qualified);
                    }
                    "function" => {
                        dependencies.functions.insert(dep_qualified);
                    }
                    "type" => {
                        dependencies.types.insert(dep_qualified);
                    }
                    _ => {} // Ignore unknown dependency types
                }
            }
            
            let (object_qualified, trigger_table) = parse_state_object_name(object_type, object_name);
            result.push((object_type.clone(), object_qualified, trigger_table, dependencies));
        }
        
        Ok(result)
    }
    
    // Helper method to convert ObjectType to string
    fn object_type_to_string(&self, object_type: &ObjectType) -> &'static str {
        match object_type {
            ObjectType::Table => "table",
            ObjectType::View => "view",
            ObjectType::MaterializedView => "materialized_view",
            ObjectType::Function => "function",
            ObjectType::Procedure => "procedure",
            ObjectType::Type => "type",
            ObjectType::Domain => "domain",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
            ObjectType::Comment => "comment",
            ObjectType::CronJob => "cron_job",
            ObjectType::Aggregate => "aggregate",
            ObjectType::Operator => "operator",
        }
    }
    
    // Helper method to format qualified names consistently
    fn format_qualified_name(&self, name: &QualifiedIdent) -> String {
        match &name.schema {
            Some(schema) => format!("{}.{}", schema, name.name),
            None => name.name.clone(),
        }
    }

    // Helper method to convert string back to ObjectType
    fn string_to_object_type(&self, s: &str) -> Option<ObjectType> {
        match s {
            "table" => Some(ObjectType::Table),
            "view" => Some(ObjectType::View),
            "materialized_view" => Some(ObjectType::MaterializedView),
            "function" => Some(ObjectType::Function),
            "procedure" => Some(ObjectType::Procedure),
            "type" => Some(ObjectType::Type),
            "domain" => Some(ObjectType::Domain),
            "index" => Some(ObjectType::Index),
            "trigger" => Some(ObjectType::Trigger),
            "comment" => Some(ObjectType::Comment),
            "cron_job" => Some(ObjectType::CronJob),
            "aggregate" => Some(ObjectType::Aggregate),
            "operator" => Some(ObjectType::Operator),
            _ => None,
        }
    }

    /// Find all managed objects that depend on the given relations (tables).
    ///
    /// This is used to identify objects that need to be pre-dropped before
    /// migrations that alter these tables.
    pub async fn find_dependents_of_relations(
        &self,
        relations: &[String],
    ) -> Result<Vec<(ObjectType, String)>, Box<dyn std::error::Error>> {
        if relations.is_empty() {
            return Ok(Vec::new());
        }

        let rows = self.client.query(
            r#"
            SELECT DISTINCT dependent_type, dependent_name
            FROM pgmg.pgmg_dependencies
            WHERE dependency_type = 'relation'
            AND dependency_name = ANY($1)
            "#,
            &[&relations],
        ).await?;

        let mut result = Vec::new();
        for row in rows {
            let dep_type_str: String = row.get(0);
            let dep_name: String = row.get(1);

            if let Some(obj_type) = self.string_to_object_type(&dep_type_str) {
                result.push((obj_type, dep_name));
            }
        }

        Ok(result)
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
            ObjectType::MaterializedView => "materialized_view",
            ObjectType::Function => "function",
            ObjectType::Procedure => "procedure",
            ObjectType::Type => "type",
            ObjectType::Domain => "domain",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
            ObjectType::Comment => "comment",
            ObjectType::CronJob => "cron_job",
            ObjectType::Aggregate => "aggregate",
            ObjectType::Operator => "operator",
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