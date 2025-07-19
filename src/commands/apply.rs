use std::path::PathBuf;
use crate::db::{StateManager, connect_with_url};
use crate::sql::{SqlObject, ObjectType, objects::{calculate_ddl_hash, extract_trigger_table}, splitter::split_sql_file};
use crate::commands::plan::{execute_plan, ChangeOperation};
use crate::config::PgmgConfig;
use crate::notify::{ObjectLoadedNotification, emit_object_loaded_notification};
use crate::plpgsql_check::{check_modified_functions, display_check_errors};
use owo_colors::OwoColorize;

#[derive(Debug)]
pub struct ApplyResult {
    pub migrations_applied: Vec<String>,
    pub objects_created: Vec<String>,
    pub objects_updated: Vec<String>,
    pub objects_deleted: Vec<String>,
    pub errors: Vec<String>,
}

pub async fn execute_apply(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
    config: &PgmgConfig,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    // Connect to database
    let (mut client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();

    // Initialize state tracking
    let state_manager = StateManager::new(&client);
    state_manager.initialize().await?;

    let mut apply_result = ApplyResult {
        migrations_applied: Vec::new(),
        objects_created: Vec::new(),
        objects_updated: Vec::new(),
        objects_deleted: Vec::new(),
        errors: Vec::new(),
    };

    // Step 1: Get the plan to understand what needs to be applied
    let plan_result = execute_plan(
        migrations_dir.clone(),
        code_dir.clone(),
        connection_string.clone(),
        None, // No graph output for apply
    ).await?;

    if plan_result.changes.is_empty() && plan_result.new_migrations.is_empty() {
        println!("{}", "No changes to apply. Database is up to date.".green());
        return Ok(apply_result);
    }

    // Step 2: Start a transaction for all changes
    let transaction = client.transaction().await?;

    // Step 3: Apply migrations first (they need to be applied in order)
    if !plan_result.new_migrations.is_empty() {
        println!("{} {} {}", "Applying".blue().bold(), plan_result.new_migrations.len().to_string().yellow(), "new migrations...".blue().bold());
        
        if let Some(ref migrations_dir) = migrations_dir {
            for migration_name in &plan_result.new_migrations {
                match apply_migration(&transaction, migrations_dir, migration_name).await {
                    Ok(_) => {
                        apply_result.migrations_applied.push(migration_name.clone());
                        println!("  {} Applied migration: {}", "✓".green().bold(), migration_name.cyan());
                    }
                    Err(e) => {
                        apply_result.errors.push(format!("Failed to apply migration {}: {}", migration_name, e));
                        println!("  {} Failed migration: {} - {}", "✗".red().bold(), migration_name.cyan(), e.to_string().red());
                    }
                }
            }
        }
    }

    // Track modified objects for plpgsql_check
    let mut modified_objects: Vec<&SqlObject> = Vec::new();
    
    // Step 4: Apply object changes based on dependency order
    if !plan_result.changes.is_empty() {
        println!("{} {} {}", "Applying".blue().bold(), plan_result.changes.len().to_string().yellow(), "object changes...".blue().bold());
        
        // Separate the changes into phases
        let (_migrations, non_migrations): (Vec<_>, Vec<_>) = plan_result.changes.iter()
            .partition(|change| matches!(change, ChangeOperation::ApplyMigration { .. }));
        
        let (creates, non_creates): (Vec<_>, Vec<_>) = non_migrations.into_iter()
            .partition(|change| matches!(change, ChangeOperation::CreateObject { .. }));
        
        let (updates, deletes): (Vec<_>, Vec<_>) = non_creates.into_iter()
            .partition(|change| matches!(change, ChangeOperation::UpdateObject { .. }));
        
        // Get dependency order if available
        let (creation_order, deletion_order) = if let Some(ref dependency_graph) = plan_result.dependency_graph {
            match (dependency_graph.creation_order(), dependency_graph.deletion_order()) {
                (Ok(create_ord), Ok(delete_ord)) => (Some(create_ord), Some(delete_ord)),
                _ => {
                    eprintln!("Warning: Could not determine dependency order. Applying changes in original order.");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        // Track if transaction has been aborted
        let mut transaction_aborted = false;

        // Phase 1: Drop all objects that need updating (in reverse dependency order)
        if !updates.is_empty() && deletion_order.is_some() {
            println!("\n{}: {}", "Phase 1".blue().bold(), "Dropping objects for update...".blue());
            let del_order = deletion_order.unwrap();
            
            // Sort updates by deletion order
            let mut ordered_updates_for_drop = Vec::new();
            for object_ref in &del_order {
                if let Some(update) = updates.iter().find(|u| match u {
                    ChangeOperation::UpdateObject { object, .. } => 
                        object.object_type == object_ref.object_type &&
                        object.qualified_name == object_ref.qualified_name,
                    _ => false,
                }) {
                    ordered_updates_for_drop.push(update);
                }
            }
            
            for change in ordered_updates_for_drop {
                if transaction_aborted { break; }
                
                if let ChangeOperation::UpdateObject { object, .. } = change {
                    match apply_drop_for_update(&transaction, object).await {
                        Ok(_) => {
                            println!("  {} Dropped {}: {} (for update)", 
                                "✓".green().bold(),
                                format!("{:?}", object.object_type).to_lowercase().yellow(),
                                format_object_name(object).cyan()
                            );
                        }
                        Err(e) => {
                            apply_result.errors.push(format!("Failed to drop {} for update: {}", format_object_name(object), e));
                            println!("  {} Failed to drop {}: {}", "✗".red().bold(), format_object_name(object).cyan(), e.to_string().red());
                            transaction_aborted = true;
                        }
                    }
                }
            }
        }
        
        // Phase 2: Delete objects marked for deletion
        if !deletes.is_empty() && !transaction_aborted {
            println!("\n{}: {}", "Phase 2".blue().bold(), "Deleting objects...".blue());
            for change in deletes {
                if transaction_aborted { break; }
                
                if let ChangeOperation::DeleteObject { object_type, object_name, .. } = change {
                    match apply_delete_object(&transaction, object_type, object_name).await {
                        Ok(_) => {
                            apply_result.objects_deleted.push(object_name.clone());
                            println!("  {} Deleted {}: {}", 
                                "✓".green().bold(),
                                format!("{:?}", object_type).to_lowercase().yellow(),
                                object_name.cyan()
                            );
                        }
                        Err(e) => {
                            apply_result.errors.push(format!("Failed to delete {}: {}", object_name, e));
                            println!("  {} Failed to delete {}: {}", "✗".red().bold(), object_name.cyan(), e.to_string().red());
                            transaction_aborted = true;
                        }
                    }
                }
            }
        }
        
        // Phase 3: Create new objects and recreate updated objects (in dependency order)
        if !transaction_aborted && (creates.len() + updates.len() > 0) {
            println!("\n{}: {}", "Phase 3".blue().bold(), "Creating objects...".blue());
            
            // Combine creates and updates (which need recreation)
            let mut all_creates: Vec<(&SqlObject, bool)> = Vec::new();
            
            // Add regular creates
            for change in &creates {
                if let ChangeOperation::CreateObject { object, .. } = change {
                    all_creates.push((object, false));
                }
            }
            
            // Add updates (which need recreation)
            for change in &updates {
                if let ChangeOperation::UpdateObject { object, .. } = change {
                    all_creates.push((object, true));
                }
            }
            
            // Sort by creation order if available
            if let Some(ref create_order) = creation_order {
                all_creates.sort_by_key(|(obj, _)| {
                    create_order.iter().position(|ref_| 
                        ref_.object_type == obj.object_type &&
                        ref_.qualified_name == obj.qualified_name
                    ).unwrap_or(usize::MAX)
                });
            }
            
            for (object, is_update) in all_creates {
                if transaction_aborted { break; }
                
                match apply_create_object(&transaction, object, config).await {
                    Ok(_) => {
                        // Track modified objects for plpgsql_check
                        modified_objects.push(object);
                        
                        if is_update {
                            apply_result.objects_updated.push(format_object_name(object));
                            println!("  {} Recreated {}: {} (updated)", 
                                "✓".green().bold(),
                                format!("{:?}", object.object_type).to_lowercase().yellow(),
                                format_object_name(object).cyan()
                            );
                        } else {
                            apply_result.objects_created.push(format_object_name(object));
                            println!("  {} Created {}: {}", 
                                "✓".green().bold(),
                                format!("{:?}", object.object_type).to_lowercase().yellow(),
                                format_object_name(object).cyan()
                            );
                        }
                    }
                    Err(e) => {
                        let action = if is_update { "recreate" } else { "create" };
                        apply_result.errors.push(format!("Failed to {} {}: {}", action, format_object_name(object), e));
                        println!("  {} Failed to {} {}: {}", "✗".red().bold(), action, format_object_name(object).cyan(), e.to_string().red());
                        transaction_aborted = true;
                    }
                }
            }
        }
    }

    // Step 4.5: Run plpgsql_check on modified functions if in development mode
    if apply_result.errors.is_empty() && 
       config.development_mode.unwrap_or(false) && 
       config.check_plpgsql.unwrap_or(false) &&
       !modified_objects.is_empty() {
        
        match check_modified_functions(&transaction, &modified_objects).await {
            Ok(check_errors) => {
                display_check_errors(&check_errors);
            }
            Err(e) => {
                // Log but don't fail the operation
                eprintln!("{}: Failed to run plpgsql_check: {}", 
                    "Warning".yellow().bold(), e);
            }
        }
    }
    
    // Step 5: Commit or rollback transaction
    if apply_result.errors.is_empty() {
        transaction.commit().await?;
        println!("{}", "All changes applied successfully!".green().bold());
    } else {
        transaction.rollback().await?;
        eprintln!("{} {} {}", "Rolled back due to".red().bold(), apply_result.errors.len().to_string().yellow(), "errors:".red().bold());
        for error in &apply_result.errors {
            eprintln!("  {} {}", "-".red().bold(), error.red());
        }
        return Err("Apply operation failed - all changes rolled back".into());
    }

    Ok(apply_result)
}


async fn apply_migration(
    client: &tokio_postgres::Transaction<'_>,
    migrations_dir: &PathBuf,
    migration_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let migration_path = migrations_dir.join(format!("{}.sql", migration_name));
    let migration_content = std::fs::read_to_string(&migration_path)?;
    
    // Split migration into statements and execute each one
    let statements = split_sql_file(&migration_content)?;
    
    for statement in statements {
        if !statement.sql.trim().is_empty() {
            client.execute(&statement.sql, &[]).await?;
        }
    }
    
    // Record migration as applied in pgmg_migrations table
    client.execute(
        "INSERT INTO pgmg_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
        &[&migration_name],
    ).await?;
    
    Ok(())
}

async fn apply_create_object(
    client: &tokio_postgres::Transaction<'_>,
    object: &SqlObject,
    config: &PgmgConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Execute the DDL statement
    client.execute(&object.ddl_statement, &[]).await?;
    
    // Update state tracking with object hash
    let ddl_hash = calculate_ddl_hash(&object.ddl_statement);
    update_object_hash_in_transaction(client, &object.object_type, &object.qualified_name, &ddl_hash).await?;
    
    // Emit NOTIFY event if in development mode
    if config.development_mode.unwrap_or(false) && config.emit_notify_events.unwrap_or(false) {
        let mut notification = ObjectLoadedNotification::from_sql_object(object);
        
        // Try to get the OID of the created object
        if let Ok(oid) = get_object_oid(client, &object.object_type, &object.qualified_name).await {
            notification.oid = Some(oid);
        }
        
        if let Err(e) = emit_object_loaded_notification(client, &notification).await {
            // Log the error but don't fail the operation
            eprintln!("Warning: Failed to emit NOTIFY event: {}", e);
        }
    }
    
    Ok(())
}

async fn apply_drop_for_update(
    client: &tokio_postgres::Transaction<'_>,
    object: &SqlObject,
) -> Result<(), Box<dyn std::error::Error>> {
    // Just drop the object - creation will happen in a separate phase
    let drop_statement = if object.object_type == ObjectType::Trigger {
        // For triggers, we need to extract the table name from the DDL
        match extract_trigger_table(&object.ddl_statement) {
            Ok(table_name) => {
                let trigger_name = match &object.qualified_name.schema {
                    Some(schema) => format!("{}.{}", schema, object.qualified_name.name),
                    None => object.qualified_name.name.clone(),
                };
                let table_full_name = match &table_name.schema {
                    Some(schema) => format!("{}.{}", schema, table_name.name),
                    None => table_name.name.clone(),
                };
                format!("DROP TRIGGER IF EXISTS {} ON {}", trigger_name, table_full_name)
            }
            Err(e) => {
                return Err(format!("Could not extract table name from trigger DDL: {}", e).into());
            }
        }
    } else {
        generate_drop_statement(&object.object_type, &object.qualified_name)
    };
    client.execute(&drop_statement, &[]).await?;
    Ok(())
}

async fn apply_delete_object(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    object_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse the qualified name
    let qualified_name = crate::sql::QualifiedIdent::from_qualified_name(object_name);
    
    // Handle comment deletion specially - comments can't be dropped, only set to NULL
    if object_type == &ObjectType::Comment {
        let comment_null_statement = generate_comment_null_statement(object_name)?;
        client.execute(&comment_null_statement, &[]).await?;
    } else {
        // Drop the object
        let drop_statement = generate_drop_statement(object_type, &qualified_name);
        client.execute(&drop_statement, &[]).await?;
    }
    
    // Remove from state tracking
    remove_object_from_state_in_transaction(client, object_type, &qualified_name).await?;
    
    Ok(())
}


fn generate_comment_null_statement(comment_identifier: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Parse comment identifiers like:
    // "table:users" -> "COMMENT ON TABLE users IS NULL"
    // "column:users.email" -> "COMMENT ON COLUMN users.email IS NULL"  
    // "function:api.get_user" -> "COMMENT ON FUNCTION api.get_user IS NULL"
    // "trigger:my_trigger:my_table" -> "COMMENT ON TRIGGER my_trigger ON my_table IS NULL"
    
    let parts: Vec<&str> = comment_identifier.split(':').collect();
    
    match parts.as_slice() {
        ["table", name] => Ok(format!("COMMENT ON TABLE {} IS NULL", name)),
        ["view", name] => Ok(format!("COMMENT ON VIEW {} IS NULL", name)),
        ["function", name] => Ok(format!("COMMENT ON FUNCTION {} IS NULL", name)),
        ["type", name] => Ok(format!("COMMENT ON TYPE {} IS NULL", name)),
        ["domain", name] => Ok(format!("COMMENT ON DOMAIN {} IS NULL", name)),
        ["column", name] => Ok(format!("COMMENT ON COLUMN {} IS NULL", name)),
        ["trigger", trigger_name, table_name] => {
            Ok(format!("COMMENT ON TRIGGER {} ON {} IS NULL", trigger_name, table_name))
        }
        _ => Err(format!("Unknown comment identifier format: {}", comment_identifier).into()),
    }
}

fn generate_drop_statement(object_type: &ObjectType, qualified_name: &crate::sql::QualifiedIdent) -> String {
    let object_type_str = match object_type {
        ObjectType::Table => "TABLE",
        ObjectType::View => "VIEW",
        ObjectType::MaterializedView => "MATERIALIZED VIEW",
        ObjectType::Function => "FUNCTION",
        ObjectType::Procedure => "PROCEDURE",
        ObjectType::Type => "TYPE",
        ObjectType::Domain => "DOMAIN",
        ObjectType::Index => "INDEX",
        ObjectType::Trigger => "TRIGGER",
        ObjectType::Comment => "COMMENT",
    };
    
    let full_name = match &qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, qualified_name.name),
        None => qualified_name.name.clone(),
    };
    
    match object_type {
        ObjectType::Function => {
            // For functions, we need to handle overloading - use CASCADE for simplicity
            format!("DROP {} IF EXISTS {} CASCADE", object_type_str, full_name)
        }
        ObjectType::Procedure => {
            // For procedures, we also need to handle overloading - use CASCADE for simplicity
            format!("DROP {} IF EXISTS {} CASCADE", object_type_str, full_name)
        }
        ObjectType::Trigger => {
            // Triggers need special handling - they require the table name
            // We'll need to return both the trigger name and table name
            // For now, return just the trigger name and we'll handle it specially
            format!("DROP TRIGGER IF EXISTS {}", full_name)
        }
        _ => {
            format!("DROP {} IF EXISTS {}", object_type_str, full_name)
        }
    }
}

async fn update_object_hash_in_transaction(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    object_name: &crate::sql::QualifiedIdent,
    ddl_hash: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_type_str = match object_type {
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
    };

    let qualified_name = match &object_name.schema {
        Some(schema) => format!("{}.{}", schema, object_name.name),
        None => object_name.name.clone(),
    };

    client.execute(
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

async fn remove_object_from_state_in_transaction(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    object_name: &crate::sql::QualifiedIdent,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_type_str = match object_type {
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
    };

    let qualified_name = match &object_name.schema {
        Some(schema) => format!("{}.{}", schema, object_name.name),
        None => object_name.name.clone(),
    };

    client.execute(
        "DELETE FROM pgmg_state WHERE object_type = $1 AND object_name = $2",
        &[&object_type_str, &qualified_name],
    ).await?;

    Ok(())
}

async fn get_object_oid(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    qualified_name: &crate::sql::QualifiedIdent,
) -> Result<u32, Box<dyn std::error::Error>> {
    let (schema_name, object_name) = match &qualified_name.schema {
        Some(s) => (s.as_str(), qualified_name.name.as_str()),
        None => ("public", qualified_name.name.as_str()),
    };
    
    let query = match object_type {
        ObjectType::Table => {
            "SELECT c.oid FROM pg_class c 
             JOIN pg_namespace n ON n.oid = c.relnamespace 
             WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'"
        }
        ObjectType::View => {
            "SELECT c.oid FROM pg_class c 
             JOIN pg_namespace n ON n.oid = c.relnamespace 
             WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'v'"
        }
        ObjectType::MaterializedView => {
            "SELECT c.oid FROM pg_class c 
             JOIN pg_namespace n ON n.oid = c.relnamespace 
             WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'm'"
        }
        ObjectType::Function => {
            "SELECT p.oid FROM pg_proc p 
             JOIN pg_namespace n ON n.oid = p.pronamespace 
             WHERE n.nspname = $1 AND p.proname = $2 AND p.prokind = 'f'"
        }
        ObjectType::Procedure => {
            "SELECT p.oid FROM pg_proc p 
             JOIN pg_namespace n ON n.oid = p.pronamespace 
             WHERE n.nspname = $1 AND p.proname = $2 AND p.prokind = 'p'"
        }
        ObjectType::Type => {
            "SELECT t.oid FROM pg_type t 
             JOIN pg_namespace n ON n.oid = t.typnamespace 
             WHERE n.nspname = $1 AND t.typname = $2 
             AND t.typtype IN ('c', 'e')"
        }
        ObjectType::Domain => {
            "SELECT t.oid FROM pg_type t 
             JOIN pg_namespace n ON n.oid = t.typnamespace 
             WHERE n.nspname = $1 AND t.typname = $2 
             AND t.typtype = 'd'"
        }
        ObjectType::Index => {
            "SELECT c.oid FROM pg_class c 
             JOIN pg_namespace n ON n.oid = c.relnamespace 
             WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'i'"
        }
        ObjectType::Trigger => {
            // Triggers don't have their own OID in pg_class, they're in pg_trigger
            // This requires both trigger name and table name, which is complex
            // For now, we'll return an error for triggers
            return Err("Trigger OID lookup not yet implemented".into());
        }
        ObjectType::Comment => {
            // Comments don't have OIDs - they're metadata attached to objects
            return Err("Comment OID lookup not applicable".into());
        }
    };
    
    let row = client.query_one(query, &[&schema_name, &object_name]).await?;
    let oid: i32 = row.get(0);
    Ok(oid as u32)
}

fn format_object_name(object: &SqlObject) -> String {
    match &object.qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, object.qualified_name.name),
        None => object.qualified_name.name.clone(),
    }
}

pub fn print_apply_summary(result: &ApplyResult) {
    println!("\n{}", "=== PGMG Apply Summary ===".bold().blue());
    
    if !result.migrations_applied.is_empty() {
        println!("\n{}:", "Migrations Applied".bold().green());
        for migration in &result.migrations_applied {
            println!("  {} {}", "✓".green().bold(), migration.cyan());
        }
    }
    
    if !result.objects_created.is_empty() {
        println!("\n{}:", "Objects Created".bold().green());
        for object in &result.objects_created {
            println!("  {} {}", "+".green().bold(), object.cyan());
        }
    }
    
    if !result.objects_updated.is_empty() {
        println!("\n{}:", "Objects Updated".bold().yellow());
        for object in &result.objects_updated {
            println!("  {} {}", "~".yellow().bold(), object.cyan());
        }
    }
    
    if !result.objects_deleted.is_empty() {
        println!("\n{}:", "Objects Deleted".bold().red());
        for object in &result.objects_deleted {
            println!("  {} {}", "-".red().bold(), object.cyan());
        }
    }
    
    if !result.errors.is_empty() {
        println!("\n{}:", "Errors".bold().red());
        for error in &result.errors {
            println!("  {} {}", "✗".red().bold(), error.red());
        }
    }
    
    let total_changes = result.migrations_applied.len() + 
                       result.objects_created.len() + 
                       result.objects_updated.len() + 
                       result.objects_deleted.len();
    
    if total_changes == 0 && result.errors.is_empty() {
        println!("\n{}", "No changes applied. Database was already up to date.".green());
    } else if result.errors.is_empty() {
        println!("\n{} {} {}", 
            "✓".green().bold(), 
            "Successfully applied".green().bold(), 
            format!("{} changes", total_changes).yellow()
        );
    } else {
        println!("\n{} {} {}", 
            "✗".red().bold(), 
            "Apply failed with".red().bold(), 
            format!("{} errors", result.errors.len()).yellow()
        );
    }
}