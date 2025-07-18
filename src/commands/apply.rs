use std::path::PathBuf;
use crate::db::{StateManager, connect_with_url};
use crate::sql::{SqlObject, ObjectType, objects::calculate_ddl_hash, splitter::split_sql_file};
use crate::commands::plan::{execute_plan, ChangeOperation};
use crate::analysis::ObjectRef;

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
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    // Connect to database
    let (mut client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

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
        println!("No changes to apply. Database is up to date.");
        return Ok(apply_result);
    }

    // Step 2: Start a transaction for all changes
    let transaction = client.transaction().await?;

    // Step 3: Apply migrations first (they need to be applied in order)
    if !plan_result.new_migrations.is_empty() {
        println!("Applying {} new migrations...", plan_result.new_migrations.len());
        
        if let Some(ref migrations_dir) = migrations_dir {
            for migration_name in &plan_result.new_migrations {
                match apply_migration(&transaction, migrations_dir, migration_name).await {
                    Ok(_) => {
                        apply_result.migrations_applied.push(migration_name.clone());
                        println!("  ✓ Applied migration: {}", migration_name);
                    }
                    Err(e) => {
                        apply_result.errors.push(format!("Failed to apply migration {}: {}", migration_name, e));
                        println!("  ✗ Failed migration: {} - {}", migration_name, e);
                    }
                }
            }
        }
    }

    // Step 4: Apply object changes based on dependency order
    if !plan_result.changes.is_empty() {
        println!("Applying {} object changes...", plan_result.changes.len());
        
        // Use the dependency graph from the plan result for proper ordering if available
        // The graph may be None if there are no SQL objects, only migrations
        let ordered_changes = if let Some(ref dependency_graph) = plan_result.dependency_graph {
            // Get the creation order from the dependency graph
            match dependency_graph.creation_order() {
                Ok(creation_order) => {
                    // Sort changes according to the dependency order
                    order_changes_by_dependencies(&plan_result.changes, &creation_order)
                }
                Err(e) => {
                    eprintln!("Warning: Could not determine dependency order: {}. Applying changes in original order.", e);
                    plan_result.changes.clone()
                }
            }
        } else {
            // No dependency graph available, use original order
            plan_result.changes.clone()
        };

        // Apply changes in dependency order
        for change in &ordered_changes {
            match change {
                ChangeOperation::ApplyMigration { .. } => {
                    // Already handled above
                    continue;
                }
                ChangeOperation::CreateObject { object, .. } => {
                    match apply_create_object(&transaction, object).await {
                        Ok(_) => {
                            apply_result.objects_created.push(format_object_name(object));
                            println!("  ✓ Created {}: {}", 
                                format!("{:?}", object.object_type).to_lowercase(),
                                format_object_name(object)
                            );
                        }
                        Err(e) => {
                            apply_result.errors.push(format!("Failed to create {}: {}", format_object_name(object), e));
                            println!("  ✗ Failed to create {}: {}", format_object_name(object), e);
                        }
                    }
                }
                ChangeOperation::UpdateObject { object, .. } => {
                    match apply_update_object(&transaction, object).await {
                        Ok(_) => {
                            apply_result.objects_updated.push(format_object_name(object));
                            println!("  ✓ Updated {}: {}", 
                                format!("{:?}", object.object_type).to_lowercase(),
                                format_object_name(object)
                            );
                        }
                        Err(e) => {
                            apply_result.errors.push(format!("Failed to update {}: {}", format_object_name(object), e));
                            println!("  ✗ Failed to update {}: {}", format_object_name(object), e);
                        }
                    }
                }
                ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                    match apply_delete_object(&transaction, object_type, object_name).await {
                        Ok(_) => {
                            apply_result.objects_deleted.push(object_name.clone());
                            println!("  ✓ Deleted {}: {}", 
                                format!("{:?}", object_type).to_lowercase(),
                                object_name
                            );
                        }
                        Err(e) => {
                            apply_result.errors.push(format!("Failed to delete {}: {}", object_name, e));
                            println!("  ✗ Failed to delete {}: {}", object_name, e);
                        }
                    }
                }
            }
        }
    }

    // Step 5: Commit or rollback transaction
    if apply_result.errors.is_empty() {
        transaction.commit().await?;
        println!("All changes applied successfully!");
    } else {
        transaction.rollback().await?;
        eprintln!("Rolled back due to {} errors:", apply_result.errors.len());
        for error in &apply_result.errors {
            eprintln!("  - {}", error);
        }
        return Err("Apply operation failed - all changes rolled back".into());
    }

    Ok(apply_result)
}

/// Order changes according to dependency graph's topological order
fn order_changes_by_dependencies(
    changes: &[ChangeOperation], 
    creation_order: &[ObjectRef]
) -> Vec<ChangeOperation> {
    let mut ordered_changes = Vec::new();
    let mut remaining_changes: Vec<ChangeOperation> = changes.to_vec();
    
    // First, add all non-object changes (migrations) to preserve their order
    let (migrations, object_changes): (Vec<_>, Vec<_>) = remaining_changes.into_iter()
        .partition(|change| matches!(change, ChangeOperation::ApplyMigration { .. }));
    
    ordered_changes.extend(migrations);
    remaining_changes = object_changes;
    
    // Then add object changes in dependency order
    for object_ref in creation_order {
        // Find the matching change for this object
        if let Some(pos) = remaining_changes.iter().position(|change| {
            match change {
                ChangeOperation::CreateObject { object, .. } => {
                    object.object_type == object_ref.object_type &&
                    object.qualified_name == object_ref.qualified_name
                }
                ChangeOperation::UpdateObject { object, .. } => {
                    object.object_type == object_ref.object_type &&
                    object.qualified_name == object_ref.qualified_name
                }
                ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                    let qualified_name = crate::sql::QualifiedIdent::from_qualified_name(object_name);
                    object_type == &object_ref.object_type &&
                    qualified_name == object_ref.qualified_name
                }
                _ => false,
            }
        }) {
            ordered_changes.push(remaining_changes.remove(pos));
        }
    }
    
    // Handle delete operations in reverse dependency order (dependents first)
    // This ensures we don't violate foreign key constraints
    let mut delete_changes: Vec<ChangeOperation> = remaining_changes.into_iter()
        .filter(|change| matches!(change, ChangeOperation::DeleteObject { .. }))
        .collect();
    
    // Reverse the order for deletions
    delete_changes.reverse();
    ordered_changes.extend(delete_changes);
    
    ordered_changes
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
) -> Result<(), Box<dyn std::error::Error>> {
    // Execute the DDL statement
    client.execute(&object.ddl_statement, &[]).await?;
    
    // Update state tracking with object hash
    let ddl_hash = calculate_ddl_hash(&object.ddl_statement);
    update_object_hash_in_transaction(client, &object.object_type, &object.qualified_name, &ddl_hash).await?;
    
    Ok(())
}

async fn apply_update_object(
    client: &tokio_postgres::Transaction<'_>,
    object: &SqlObject,
) -> Result<(), Box<dyn std::error::Error>> {
    // For updates, we need to drop and recreate
    // This is a simplified approach - in production you might want more sophisticated handling
    
    // Drop the existing object first
    let drop_statement = generate_drop_statement(&object.object_type, &object.qualified_name);
    client.execute(&drop_statement, &[]).await?;
    
    // Create the new version
    client.execute(&object.ddl_statement, &[]).await?;
    
    // Update state tracking with new hash
    let ddl_hash = calculate_ddl_hash(&object.ddl_statement);
    update_object_hash_in_transaction(client, &object.object_type, &object.qualified_name, &ddl_hash).await?;
    
    Ok(())
}

async fn apply_delete_object(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    object_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse the qualified name
    let qualified_name = crate::sql::QualifiedIdent::from_qualified_name(object_name);
    
    // Drop the object
    let drop_statement = generate_drop_statement(object_type, &qualified_name);
    client.execute(&drop_statement, &[]).await?;
    
    // Remove from state tracking
    remove_object_from_state_in_transaction(client, object_type, &qualified_name).await?;
    
    Ok(())
}

fn generate_drop_statement(object_type: &ObjectType, qualified_name: &crate::sql::QualifiedIdent) -> String {
    let object_type_str = match object_type {
        ObjectType::Table => "TABLE",
        ObjectType::View => "VIEW",
        ObjectType::Function => "FUNCTION",
        ObjectType::Type => "TYPE",
        ObjectType::Domain => "DOMAIN",
        ObjectType::Index => "INDEX",
        ObjectType::Trigger => "TRIGGER",
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

    client.execute(
        "DELETE FROM pgmg_state WHERE object_type = $1 AND object_name = $2",
        &[&object_type_str, &qualified_name],
    ).await?;

    Ok(())
}

fn format_object_name(object: &SqlObject) -> String {
    match &object.qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, object.qualified_name.name),
        None => object.qualified_name.name.clone(),
    }
}

pub fn print_apply_summary(result: &ApplyResult) {
    println!("\n=== PGMG Apply Summary ===");
    
    if !result.migrations_applied.is_empty() {
        println!("\nMigrations Applied:");
        for migration in &result.migrations_applied {
            println!("  ✓ {}", migration);
        }
    }
    
    if !result.objects_created.is_empty() {
        println!("\nObjects Created:");
        for object in &result.objects_created {
            println!("  + {}", object);
        }
    }
    
    if !result.objects_updated.is_empty() {
        println!("\nObjects Updated:");
        for object in &result.objects_updated {
            println!("  ~ {}", object);
        }
    }
    
    if !result.objects_deleted.is_empty() {
        println!("\nObjects Deleted:");
        for object in &result.objects_deleted {
            println!("  - {}", object);
        }
    }
    
    if !result.errors.is_empty() {
        println!("\nErrors:");
        for error in &result.errors {
            println!("  ✗ {}", error);
        }
    }
    
    let total_changes = result.migrations_applied.len() + 
                       result.objects_created.len() + 
                       result.objects_updated.len() + 
                       result.objects_deleted.len();
    
    if total_changes == 0 && result.errors.is_empty() {
        println!("\nNo changes applied. Database was already up to date.");
    } else if result.errors.is_empty() {
        println!("\n✓ Successfully applied {} changes", total_changes);
    } else {
        println!("\n✗ Apply failed with {} errors", result.errors.len());
    }
}