use std::path::PathBuf;
use std::time::Duration;
use std::collections::HashSet;
use crate::db::{StateManager, connect_with_url, AdvisoryLockManager, AdvisoryLockError};
use crate::sql::{SqlObject, ObjectType, objects::{calculate_ddl_hash, extract_trigger_table}, splitter::split_sql_file};
use crate::commands::plan::{execute_plan, ChangeOperation};
use crate::config::PgmgConfig;
use crate::analysis::ObjectRef;
use crate::notify::{ObjectLoadedNotification, emit_object_loaded_notification};
use crate::plpgsql_check::{check_modified_functions, check_soft_dependent_functions, display_check_errors};
use crate::error::format_postgres_error_with_details;
use tracing::{info, warn, debug, error};

#[cfg(feature = "cli")]
use owo_colors::OwoColorize;

#[derive(Debug)]
pub struct ApplyResult {
    pub migrations_applied: Vec<String>,
    pub objects_created: Vec<String>,
    pub objects_updated: Vec<String>,
    pub objects_deleted: Vec<String>,
    pub errors: Vec<String>,
    pub plpgsql_errors_found: usize,
    pub plpgsql_warnings_found: usize,
}

pub async fn execute_apply(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
    config: &PgmgConfig,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    execute_apply_with_test_mode(migrations_dir, code_dir, connection_string, config, false).await
}

/// Execute apply with test mode support
pub async fn execute_apply_with_test_mode(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
    config: &PgmgConfig,
    test_mode: bool,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    execute_apply_with_lock_management(migrations_dir, code_dir, connection_string, config, test_mode).await
}

/// Library-friendly version of execute_apply
/// 
/// All output goes through the tracing system. Configure your tracing subscriber
/// to control how output is displayed or logged.
/// 
/// # Example
/// ```no_run
/// use pgmg::{PgmgConfig, apply_migrations};
/// use tracing_subscriber;
/// 
/// // Initialize tracing (you control the output format)
/// tracing_subscriber::fmt::init();
/// 
/// let config = PgmgConfig::load_from_file()?;
/// let result = apply_migrations(&config).await?;
/// ```
pub async fn apply_migrations(
    config: &PgmgConfig,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    apply_migrations_with_options(config, None, None).await
}

/// Library-friendly version with custom directory options
pub async fn apply_migrations_with_options(
    config: &PgmgConfig,
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    use tracing::{info_span, Instrument};
    
    // Get connection string from config
    let connection_string = config.connection_string.clone()
        .ok_or("No database connection string configured")?;
    
    info!("Starting database migrations");
    debug!(?migrations_dir, ?code_dir, "Migration directories");
    
    // Use default directories if not provided
    let migrations_dir = migrations_dir.or_else(|| config.migrations_dir.clone());
    let code_dir = code_dir.or_else(|| config.code_dir.clone());
    
    // Execute with detailed tracing
    let span = info_span!("apply_migrations");
    let result = execute_apply_with_lock_management(
        migrations_dir,
        code_dir,
        connection_string,
        config,
        false, // test_mode = false for normal apply
    ).instrument(span).await?;
    
    // Log summary information
    info!(
        migrations_applied = result.migrations_applied.len(),
        objects_created = result.objects_created.len(),
        objects_updated = result.objects_updated.len(),
        objects_deleted = result.objects_deleted.len(),
        "Migration completed successfully"
    );
    
    // Log details at debug level
    for migration in &result.migrations_applied {
        debug!(migration, "Applied migration");
    }
    for object in &result.objects_created {
        debug!(object, "Created object");
    }
    for object in &result.objects_updated {
        debug!(object, "Updated object");
    }
    for object in &result.objects_deleted {
        debug!(object, "Deleted object");
    }
    
    // Log any errors that were collected
    for error in &result.errors {
        error!(error, "Migration error");
    }
    
    // Warnings for PL/pgSQL issues
    if result.plpgsql_errors_found > 0 {
        error!(
            errors = result.plpgsql_errors_found,
            "PL/pgSQL errors found - functions may not work correctly"
        );
    }
    if result.plpgsql_warnings_found > 0 {
        warn!(
            warnings = result.plpgsql_warnings_found,
            "PL/pgSQL warnings found"
        );
    }
    
    Ok(result)
}

/// Execute apply with advisory lock management
async fn execute_apply_with_lock_management(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
    config: &PgmgConfig,
    test_mode: bool,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    // Connect to database
    let (client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();
    
    // Pass test_mode through to the inner function
    execute_apply_inner(client, migrations_dir, code_dir, connection_string, config, test_mode).await
}

async fn execute_apply_inner(
    mut client: tokio_postgres::Client,
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
    config: &PgmgConfig,
    test_mode: bool,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {

    // Acquire advisory lock to prevent concurrent apply operations
    let mut lock_manager = AdvisoryLockManager::new(&connection_string);
    
    // Try to acquire lock with 30-second timeout
    match lock_manager.acquire_lock(&client, Duration::from_secs(30)).await {
        Ok(()) => {
            info!("Acquired concurrency lock for apply operation");
        }
        Err(AdvisoryLockError::Timeout { timeout_seconds }) => {
            return Err(format!(
                "Could not acquire lock for apply operation after {} seconds.\n\
                Another pgmg apply process may be running against this database.\n\
                If you're sure no other process is running, the lock may be stale and will be cleaned up when that session ends.",
                timeout_seconds
            ).into());
        }
        Err(e) => {
            return Err(format!("Failed to acquire advisory lock: {}", e).into());
        }
    }

    // Execute the apply operation
    let apply_result = execute_apply_internal(
        migrations_dir,
        code_dir,
        connection_string,
        config,
        &mut client,
        test_mode,
    ).await;

    // Always attempt to release the lock
    if let Err(e) = lock_manager.release_lock(&client).await {
        warn!("Failed to release advisory lock: {}", e);
        // Don't fail the operation if lock release fails - it will be cleaned up by PostgreSQL
    }

    apply_result
}


/// Internal apply function that runs with the lock already acquired
async fn execute_apply_internal(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
    config: &PgmgConfig,
    client: &mut tokio_postgres::Client,
    test_mode: bool,
) -> Result<ApplyResult, Box<dyn std::error::Error>> {

    // Initialize state tracking
    let state_manager = StateManager::new(client);
    state_manager.initialize().await?;

    let mut apply_result = ApplyResult {
        migrations_applied: Vec::new(),
        objects_created: Vec::new(),
        objects_updated: Vec::new(),
        objects_deleted: Vec::new(),
        errors: Vec::new(),
        plpgsql_errors_found: 0,
        plpgsql_warnings_found: 0,
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

    // Step 2.5: Pre-drop managed objects if there are migrations
    // This unblocks migrations that would otherwise be blocked by dependent objects
    let mut pre_dropped_objects: HashSet<String> = HashSet::new();

    if !plan_result.new_migrations.is_empty() && !plan_result.changes.is_empty() {
        // Collect objects that need pre-dropping (updates and deletes)
        let updates_to_predrop: Vec<&ChangeOperation> = plan_result.changes.iter()
            .filter(|change| matches!(change, ChangeOperation::UpdateObject { .. }))
            .collect();

        let deletes_to_predrop: Vec<&ChangeOperation> = plan_result.changes.iter()
            .filter(|change| matches!(change, ChangeOperation::DeleteObject { .. }))
            .collect();

        if !updates_to_predrop.is_empty() || !deletes_to_predrop.is_empty() {
            if !test_mode {
                println!("{} {} {}",
                    "Pre-dropping".blue().bold(),
                    (updates_to_predrop.len() + deletes_to_predrop.len()).to_string().yellow(),
                    "managed objects to unblock migrations...".blue().bold()
                );
            }

            // Get dependency order for proper dropping
            let deletion_order = plan_result.dependency_graph.as_ref()
                .and_then(|g| g.deletion_order().ok());

            // Phase A: Drop objects for update (reverse dependency order)
            if !updates_to_predrop.is_empty() {
                let ordered_updates = order_changes_by_deletion(&updates_to_predrop, &deletion_order);

                for change in ordered_updates {
                    if let ChangeOperation::UpdateObject { object, .. } = change {
                        match apply_drop_for_update(&transaction, object).await {
                            Ok(_) => {
                                // Track that we pre-dropped this object
                                pre_dropped_objects.insert(format!("{:?}:{}",
                                    object.object_type,
                                    format_object_name(object)
                                ));

                                if !test_mode {
                                    println!("  {} Pre-dropped {}: {} (will be recreated after migration)",
                                        "✓".green().bold(),
                                        format!("{:?}", object.object_type).to_lowercase().yellow(),
                                        format_object_name(object).cyan()
                                    );
                                }
                            }
                            Err(e) => {
                                apply_result.errors.push(format!("Failed to pre-drop {} for update: {}", format_object_name(object), e));
                                if !test_mode {
                                    println!("  {} Failed to pre-drop {}: {}", "✗".red().bold(), format_object_name(object).cyan(), e.to_string().red());
                                }
                                transaction.rollback().await?;
                                return Err("Pre-drop failed - all changes rolled back".into());
                            }
                        }
                    }
                }
            }

            // Phase B: Delete objects marked for deletion (reverse dependency order)
            if !deletes_to_predrop.is_empty() {
                let ordered_deletes = order_changes_by_deletion(&deletes_to_predrop, &deletion_order);

                for change in ordered_deletes {
                    if let ChangeOperation::DeleteObject { object_type, object_name, .. } = change {
                        match apply_delete_object(&transaction, object_type, object_name).await {
                            Ok(_) => {
                                // Track that we pre-dropped this object
                                pre_dropped_objects.insert(format!("{:?}:{}", object_type, object_name));

                                // Mark as deleted in result
                                apply_result.objects_deleted.push(object_name.clone());

                                if !test_mode {
                                    println!("  {} Pre-dropped {}: {} (will be deleted)",
                                        "✓".green().bold(),
                                        format!("{:?}", object_type).to_lowercase().yellow(),
                                        object_name.cyan()
                                    );
                                }
                            }
                            Err(e) => {
                                apply_result.errors.push(format!("Failed to pre-drop {}: {}", object_name, e));
                                if !test_mode {
                                    println!("  {} Failed to pre-drop {}: {}", "✗".red().bold(), object_name.cyan(), e.to_string().red());
                                }
                                transaction.rollback().await?;
                                return Err("Pre-drop failed - all changes rolled back".into());
                            }
                        }
                    }
                }
            }

            if !test_mode && (!updates_to_predrop.is_empty() || !deletes_to_predrop.is_empty()) {
                println!();  // Blank line for readability
            }
        }
    }

    // Step 3: Apply migrations first (they need to be applied in order)
    if !plan_result.new_migrations.is_empty() {
        if !test_mode {
            println!("{} {} {}", "Applying".blue().bold(), plan_result.new_migrations.len().to_string().yellow(), "new migrations...".blue().bold());
        }
        
        if let Some(ref migrations_dir) = migrations_dir {
            for migration_name in &plan_result.new_migrations {
                match apply_migration(&transaction, migrations_dir, migration_name, test_mode).await {
                    Ok(_) => {
                        apply_result.migrations_applied.push(migration_name.clone());
                        if !test_mode {
                            println!("  {} Applied migration: {}", "✓".green().bold(), migration_name.cyan());
                        }
                    }
                    Err(e) => {
                        // The error from apply_migration already contains detailed formatting
                        apply_result.errors.push(e.to_string());
                        println!("  {} Failed migration: {}", "✗".red().bold(), migration_name.cyan());
                        println!("{}", e.to_string().red());
                        break; // Stop processing migrations on first error
                    }
                }
            }
        }
    }

    // Check for migration errors before proceeding to object changes
    if !apply_result.errors.is_empty() {
        transaction.rollback().await?;
        eprintln!("\n{} {} {}", "Rolled back due to".red().bold(), apply_result.errors.len().to_string().yellow(), "migration error:".red().bold());
        for error in &apply_result.errors {
            eprintln!("{}", error);
        }
        return Err("Migration failed - all changes rolled back".into());
    }

    // Track modified objects for plpgsql_check
    let mut modified_objects: Vec<&SqlObject> = Vec::new();
    
    // Step 4: Apply object changes based on dependency order
    if !plan_result.changes.is_empty() {
        if !test_mode {
            println!("{} {} {}", "Applying".blue().bold(), plan_result.changes.len().to_string().yellow(), "object changes...".blue().bold());
        }
        
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

        // Phase 1: Drop objects that need updating (in reverse dependency order)
        if !updates.is_empty() && deletion_order.is_some() {
            if !test_mode {
                println!("\n{}: {}", "Phase 1".blue().bold(), "Dropping objects for update...".blue());
            }
            let del_order = deletion_order.as_ref().expect("deletion_order was checked to be Some");
            
            // Sort updates by deletion order
            let mut ordered_updates_for_drop = Vec::new();
            for object_ref in del_order {
                if let Some(update) = updates.iter().find(|u| match u {
                    ChangeOperation::UpdateObject { object, .. } => 
                        object.object_type == object_ref.object_type &&
                        object.qualified_name == object_ref.qualified_name,
                    _ => false,
                }) {
                    ordered_updates_for_drop.push(*update);
                }
            }
            
            for change in ordered_updates_for_drop {
                if transaction_aborted { break; }

                if let ChangeOperation::UpdateObject { object, .. } = change {
                    // Skip if already pre-dropped
                    let object_key = format!("{:?}:{}", object.object_type, format_object_name(object));
                    if pre_dropped_objects.contains(&object_key) {
                        continue;
                    }

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
        
        // Phase 2: Delete objects marked for deletion (in dependency order)
        if !deletes.is_empty() && !transaction_aborted {
            if !test_mode {
                println!("\n{}: {}", "Phase 2".blue().bold(), "Deleting objects...".blue());
            }
            
            // Separate comments from other deletions
            let (comment_deletes, non_comment_deletes): (Vec<_>, Vec<_>) = deletes.into_iter()
                .partition(|change| match change {
                    ChangeOperation::DeleteObject { object_type, .. } => object_type == &ObjectType::Comment,
                    _ => false,
                });
            
            // Process comments first (they need their parent objects to exist)
            if !comment_deletes.is_empty() {
                println!("  {}", "Processing comments first...".dimmed());
                for change in comment_deletes {
                    if transaction_aborted { break; }

                    if let ChangeOperation::DeleteObject { object_type, object_name, .. } = change {
                        // Skip if already pre-dropped
                        let object_key = format!("{:?}:{}", object_type, object_name);
                        if pre_dropped_objects.contains(&object_key) {
                            continue;
                        }

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
            
            // Then process non-comment deletions in dependency order
            if !non_comment_deletes.is_empty() && !transaction_aborted {
                // Sort deletions by dependency order if available
                let ordered_deletes = if let Some(ref del_order) = deletion_order {
                    let mut ordered = Vec::new();
                    // Process in deletion order (dependents first)
                    for object_ref in del_order {
                        if let Some(delete_op) = non_comment_deletes.iter().find(|d| match d {
                            ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                                let qname = crate::sql::QualifiedIdent::from_qualified_name(object_name);
                                object_type == &object_ref.object_type && qname == object_ref.qualified_name
                            }
                            _ => false,
                        }) {
                            ordered.push(*delete_op);
                        }
                    }
                    // Add any deletes not in the dependency graph at the end
                    for delete_op in &non_comment_deletes {
                        let already_added = ordered.iter().any(|o| {
                            match (o, delete_op) {
                                (ChangeOperation::DeleteObject { object_type: t1, object_name: n1, .. },
                                 ChangeOperation::DeleteObject { object_type: t2, object_name: n2, .. }) => {
                                    t1 == t2 && n1 == n2
                                }
                                _ => false,
                            }
                        });
                        if !already_added {
                            ordered.push(*delete_op);
                        }
                    }
                    ordered
                } else {
                    non_comment_deletes
                };
                
                for change in ordered_deletes {
                    if transaction_aborted { break; }

                    if let ChangeOperation::DeleteObject { object_type, object_name, .. } = change {
                        // Skip if already pre-dropped
                        let object_key = format!("{:?}:{}", object_type, object_name);
                        if pre_dropped_objects.contains(&object_key) {
                            continue;
                        }

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
        }
        
        // Phase 3: Create new objects and recreate updated objects (in dependency order)
        if !transaction_aborted && (creates.len() + updates.len() > 0) {
            if !test_mode {
                println!("\n{}: {}", "Phase 3".blue().bold(), "Creating objects...".blue());
            }
            
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
                
                match apply_create_object(&transaction, object, config, test_mode).await {
                    Ok(_) => {
                        // Track modified objects for plpgsql_check
                        modified_objects.push(object);
                        
                        if is_update {
                            apply_result.objects_updated.push(format_object_name(object));
                            if !test_mode {
                                println!("  {} Recreated {}: {} (updated)", 
                                    "✓".green().bold(),
                                    format!("{:?}", object.object_type).to_lowercase().yellow(),
                                    format_object_name(object).cyan()
                                );
                            }
                        } else {
                            apply_result.objects_created.push(format_object_name(object));
                            if !test_mode {
                                println!("  {} Created {}: {}", 
                                    "✓".green().bold(),
                                    format!("{:?}", object.object_type).to_lowercase().yellow(),
                                    format_object_name(object).cyan()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        let action = if is_update { "recreate" } else { "create" };
                        
                        // Try to downcast to tokio_postgres::Error for detailed formatting
                        let detailed_error = if let Some(pg_err) = e.downcast_ref::<tokio_postgres::Error>() {
                            format_postgres_error_with_details(
                                &format_object_name(object),
                                object.source_file.as_deref(),
                                object.start_line,
                                &object.ddl_statement,
                                pg_err
                            )
                        } else {
                            format!("Failed to {} {}: {}", action, format_object_name(object), e)
                        };
                        
                        apply_result.errors.push(detailed_error.clone());
                        println!("  {} {}", "✗".red().bold(), detailed_error);
                        transaction_aborted = true;
                    }
                }
            }
        }
    }

    // Handle SQL errors with rollback first
    if !apply_result.errors.is_empty() {
        transaction.rollback().await?;
        eprintln!("{} {} {}", "Rolled back due to".red().bold(), apply_result.errors.len().to_string().yellow(), "errors:".red().bold());
        for error in &apply_result.errors {
            eprintln!("  {} {}", "-".red().bold(), error.red());
        }
        return Err("Apply operation failed - all changes rolled back".into());
    }
    
    // Step 4.5: Run plpgsql_check on modified functions if in development mode
    // IMPORTANT: Run plpgsql_check WITHIN the transaction before committing
    if config.development_mode.unwrap_or(false) && 
       config.check_plpgsql.unwrap_or(false) &&
       !modified_objects.is_empty() {
        
        // Collect all plpgsql_check errors before displaying
        let mut all_plpgsql_errors = Vec::new();
        
        // Check the modified functions themselves using the transaction
        match check_modified_functions(&transaction, &modified_objects).await {
            Ok(mut check_errors) => {
                for error in &check_errors {
                    if let Some(level) = &error.check_result.level {
                        match level.as_str() {
                            "error" => apply_result.plpgsql_errors_found += 1,
                            "warning" => apply_result.plpgsql_warnings_found += 1,
                            _ => {}
                        }
                    }
                }
                all_plpgsql_errors.append(&mut check_errors);
            }
            Err(e) => {
                // Log but don't fail the operation
                eprintln!("{}: Failed to run plpgsql_check: {}", 
                    "Warning".yellow().bold(), e);
            }
        }
        
        // Also check soft dependents if we have a dependency graph
        if let Some(ref dependency_graph) = plan_result.dependency_graph {
            match check_soft_dependent_functions(
                &transaction,
                dependency_graph, 
                &modified_objects,
                &plan_result.file_objects
            ).await {
                Ok(mut check_errors) => {
                    for error in &check_errors {
                        if let Some(level) = &error.check_result.level {
                            match level.as_str() {
                                "error" => apply_result.plpgsql_errors_found += 1,
                                "warning" => apply_result.plpgsql_warnings_found += 1,
                                _ => {}
                            }
                        }
                    }
                    all_plpgsql_errors.append(&mut check_errors);
                }
                Err(e) => {
                    // Log but don't fail the operation
                    eprintln!("{}: Failed to check dependent functions: {}", 
                        "Warning".yellow().bold(), e);
                }
            }
        }
        
        // Display all plpgsql_check errors at once (sorted by severity)
        if !all_plpgsql_errors.is_empty() {
            display_check_errors(&all_plpgsql_errors);
        }
        
        // If there are plpgsql_check errors, rollback and fail
        if apply_result.plpgsql_errors_found > 0 {
            transaction.rollback().await?;
            eprintln!("\n{} {} {}", 
                "Apply blocked due to".red().bold(), 
                apply_result.plpgsql_errors_found.to_string().yellow(), 
                "PL/pgSQL errors. All changes rolled back. Fix the errors above and try again.".red().bold()
            );
            return Err("Apply operation blocked due to PL/pgSQL compilation errors".into());
        }
        
        // If plpgsql_check passed, commit the transaction
        transaction.commit().await?;
        print_apply_success_message(&apply_result, test_mode);
    } else {
        // Step 5: Commit transaction if no plpgsql_check
        transaction.commit().await?;
        print_apply_success_message(&apply_result, test_mode);
    }

    Ok(apply_result)
}


async fn apply_migration(
    client: &tokio_postgres::Transaction<'_>,
    migrations_dir: &PathBuf,
    migration_name: &str,
    test_mode: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let migration_path = migrations_dir.join(format!("{}.sql", migration_name));
    let migration_content = std::fs::read_to_string(&migration_path)?;
    
    // Split migration into statements and execute each one
    let statements = split_sql_file(&migration_content)?;
    
    // Check if we're on AWS RDS once at the beginning
    let is_rds = is_aws_rds(client.client()).await;
    if is_rds {
        info!("Detected AWS RDS environment - will skip plpgsql_check related statements");
    }
    
    for (idx, statement) in statements.iter().enumerate() {
        if !statement.sql.trim().is_empty() {
            // Skip pg_cron related statements in test mode
            if test_mode && should_skip_in_test_mode(&statement.sql) {
                debug!("Skipping pg_cron statement in test mode: {}", statement.sql.lines().next().unwrap_or(""));
                continue;
            }
            
            // Skip plpgsql_check related statements on RDS
            if is_rds && should_skip_plpgsql_check_on_rds(&statement.sql) {
                debug!("Skipping plpgsql_check statement on RDS: {}", statement.sql.lines().next().unwrap_or(""));
                eprintln!("  {} Skipping plpgsql_check statement (not available on AWS RDS)", "⚠".yellow().bold());
                continue;
            }
            
            match client.execute(&statement.sql, &[]).await {
                Ok(_) => {},
                Err(e) => {
                    // Create a detailed error message with context
                    let detailed_error = format_postgres_error_with_details(
                        &format!("migration {} (statement {})", migration_name, idx + 1),
                        Some(&migration_path),
                        statement.start_line,
                        &statement.sql,
                        &e
                    );
                    return Err(detailed_error.into());
                }
            }
        }
    }
    
    // Record migration as applied in pgmg_migrations table
    client.execute(
        "INSERT INTO pgmg.pgmg_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
        &[&migration_name],
    ).await?;
    
    Ok(())
}

async fn apply_create_object(
    client: &tokio_postgres::Transaction<'_>,
    object: &SqlObject,
    config: &PgmgConfig,
    test_mode: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Skip pg_cron related objects in test mode
    if test_mode && should_skip_in_test_mode(&object.ddl_statement) {
        debug!("Skipping pg_cron object in test mode: {}", object.qualified_name.name);
        return Ok(());
    }
    
    // Execute the DDL statement
    client.execute(&object.ddl_statement, &[]).await?;
    
    // Update state tracking with object hash
    let ddl_hash = calculate_ddl_hash(&object.ddl_statement);
    update_object_hash_in_transaction(client, &object.object_type, &object.qualified_name, &ddl_hash).await?;
    
    // Store object dependencies
    store_object_dependencies_in_transaction(client, &object.object_type, &object.qualified_name, &object.dependencies).await?;
    
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
    // Handle special cases for object types that can't be dropped normally
    if object.object_type == ObjectType::Comment {
        // Comments can't be dropped, only set to NULL
        let comment_null_statement = generate_comment_null_statement_from_object(object)?;
        client.execute(&comment_null_statement, &[]).await?;
        return Ok(());
    }
    
    // Just drop the object - creation will happen in a separate phase
    let drop_statement = match object.object_type {
        ObjectType::Trigger => {
            // For triggers, we need to extract the table name from the DDL
            match extract_trigger_table(&object.ddl_statement) {
                Ok(table_name) => {
                    let trigger_name = quote_qualified_identifier(
                        object.qualified_name.schema.as_deref(),
                        &object.qualified_name.name
                    );
                    let table_full_name = quote_qualified_identifier(
                        table_name.schema.as_deref(),
                        &table_name.name
                    );
                    format!("DROP TRIGGER IF EXISTS {} ON {}", trigger_name, table_full_name)
                }
                Err(e) => {
                    return Err(format!("Could not extract table name from trigger DDL: {}", e).into());
                }
            }
        }
        ObjectType::Function | ObjectType::Procedure | ObjectType::Aggregate => {
            // For functions, procedures, and aggregates, we need to drop all existing overloads
            let existing_signatures = get_existing_function_signatures(client, &object.object_type, &object.qualified_name).await?;
            
            if existing_signatures.is_empty() {
                // No existing function found, nothing to drop
                return Ok(());
            }
            
            let object_type_str = match object.object_type {
                ObjectType::Function => "FUNCTION",
                ObjectType::Procedure => "PROCEDURE",
                ObjectType::Aggregate => "AGGREGATE",
                _ => unreachable!(),
            };
            
            // Drop all existing overloads
            for signature in existing_signatures {
                let drop_statement = format!("DROP {} IF EXISTS {}", object_type_str, signature);
                client.execute(&drop_statement, &[]).await?;
            }
            
            return Ok(());
        }
        _ => generate_drop_statement(&object.object_type, &object.qualified_name)
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
        
        // Create a savepoint before attempting the comment deletion
        client.execute("SAVEPOINT comment_deletion", &[]).await?;
        
        // Try to execute the comment deletion
        match client.execute(&comment_null_statement, &[]).await {
            Ok(_) => {
                // Comment successfully deleted, release the savepoint
                client.execute("RELEASE SAVEPOINT comment_deletion", &[]).await?;
            }
            Err(_) => {
                // Any error in comment deletion - just rollback the savepoint
                // The comment is either already gone or can't be deleted
                client.execute("ROLLBACK TO SAVEPOINT comment_deletion", &[]).await?;
            }
        }
        
        // Always remove from state tracking, regardless of whether the SQL succeeded
        // This ensures we don't try to delete non-existent comments repeatedly
        remove_object_from_state_in_transaction(client, object_type, &qualified_name).await?;
        return Ok(());
    } else if matches!(object_type, ObjectType::Function | ObjectType::Procedure | ObjectType::Aggregate | ObjectType::Operator) {
        // For functions, procedures, aggregates, and operators, drop all existing overloads
        let existing_signatures = get_existing_function_signatures(client, object_type, &qualified_name).await?;
        
        if !existing_signatures.is_empty() {
            let object_type_str = match object_type {
                ObjectType::Function => "FUNCTION",
                ObjectType::Procedure => "PROCEDURE",
                ObjectType::Aggregate => "AGGREGATE",
                ObjectType::Operator => "OPERATOR",
                _ => unreachable!(),
            };
            
            // Drop all existing overloads
            for signature in existing_signatures {
                let drop_statement = format!("DROP {} IF EXISTS {}", object_type_str, signature);
                match client.execute(&drop_statement, &[]).await {
                    Ok(_) => {},
                    Err(e) => {
                        // Check if this is a dependency error
                        let error_msg = e.to_string();
                        if error_msg.contains("because other objects depend on it") {
                            return Err(format!(
                                "Cannot drop {} {} because other objects depend on it. This usually means there are triggers, views, or other functions that reference this {}. You may need to drop those dependent objects first or review your migration strategy.",
                                object_type_str.to_lowercase(),
                                signature,
                                object_type_str.to_lowercase()
                            ).into());
                        } else {
                            // Re-throw the original error
                            return Err(e.into());
                        }
                    }
                }
            }
        }
    } else if object_type == &ObjectType::Trigger {
        // Triggers need special handling - we need to find the table they're on
        let trigger_table = get_trigger_table_from_dependencies(client, &qualified_name).await?;
        let trigger_name = quote_qualified_identifier(
            qualified_name.schema.as_deref(),
            &qualified_name.name
        );
        
        // The trigger_table could be either "table_name" or "schema.table_name"
        // We need to properly quote it
        let quoted_table = if trigger_table.contains('.') {
            // It's already qualified, split and quote each part
            let parts: Vec<&str> = trigger_table.splitn(2, '.').collect();
            if parts.len() == 2 {
                quote_qualified_identifier(Some(parts[0]), parts[1])
            } else {
                quote_qualified_identifier(None, &trigger_table)
            }
        } else {
            quote_qualified_identifier(None, &trigger_table)
        };
        
        let drop_statement = format!("DROP TRIGGER IF EXISTS {} ON {}", trigger_name, quoted_table);
        client.execute(&drop_statement, &[]).await?;
    } else {
        // Drop the object
        let drop_statement = generate_drop_statement(object_type, &qualified_name);
        client.execute(&drop_statement, &[]).await?;
    }
    
    // Remove from state tracking
    remove_object_from_state_in_transaction(client, object_type, &qualified_name).await?;
    
    Ok(())
}


fn generate_comment_null_statement_from_object(object: &SqlObject) -> Result<String, Box<dyn std::error::Error>> {
    let comment_identifier = &object.qualified_name.name;
    
    // Parse comment identifiers like:
    // "table:users" -> "COMMENT ON TABLE users IS NULL"
    // "column:users.email" -> "COMMENT ON COLUMN users.email IS NULL"  
    // "function:api.get_user" -> "COMMENT ON FUNCTION api.get_user IS NULL"
    // "trigger:my_trigger:my_table" -> "COMMENT ON TRIGGER my_trigger ON my_table IS NULL"
    // "aggregate:my_aggregate" -> "COMMENT ON AGGREGATE my_aggregate IS NULL"
    
    let parts: Vec<&str> = comment_identifier.split(':').collect();
    
    match parts.as_slice() {
        ["table", name] => Ok(format!("COMMENT ON TABLE {} IS NULL", name)),
        ["view", name] => Ok(format!("COMMENT ON VIEW {} IS NULL", name)),
        ["materialized_view", name] => Ok(format!("COMMENT ON MATERIALIZED VIEW {} IS NULL", name)),
        ["function", name] => {
            // Since pgmg prevents function overloading, we can use the name without parentheses
            // Remove any trailing () if present in the identifier
            let func_name = name.trim_end_matches("()");
            Ok(format!("COMMENT ON FUNCTION {} IS NULL", func_name))
        }
        ["type", name] => Ok(format!("COMMENT ON TYPE {} IS NULL", name)),
        ["domain", name] => Ok(format!("COMMENT ON DOMAIN {} IS NULL", name)),
        ["column", name] => Ok(format!("COMMENT ON COLUMN {} IS NULL", name)),
        ["trigger", trigger_name, table_name] => {
            Ok(format!("COMMENT ON TRIGGER {} ON {} IS NULL", trigger_name, table_name))
        }
        ["aggregate", name] => {
            // Since pgmg prevents aggregate overloading, we can use the name without parentheses
            let agg_name = name.trim_end_matches("()");
            Ok(format!("COMMENT ON AGGREGATE {} IS NULL", agg_name))
        }
        ["procedure", name] => {
            // Since pgmg prevents procedure overloading, we can use the name without parentheses
            let proc_name = name.trim_end_matches("()");
            Ok(format!("COMMENT ON PROCEDURE {} IS NULL", proc_name))
        }
        ["operator", name] => {
            // Operators need their full signature, which should be stored in the comment identifier
            // Format: operator:name(lefttype,righttype)
            Ok(format!("COMMENT ON OPERATOR {} IS NULL", name))
        }
        _ => Err(format!("Unknown comment identifier format: {}", comment_identifier).into()),
    }
}

fn generate_comment_null_statement(comment_identifier: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Parse comment identifiers like:
    // "table:users" -> "COMMENT ON TABLE users IS NULL"
    // "column:users.email" -> "COMMENT ON COLUMN users.email IS NULL"  
    // "function:api.get_user" -> "COMMENT ON FUNCTION api.get_user IS NULL"
    // "trigger:my_trigger:my_table" -> "COMMENT ON TRIGGER my_trigger ON my_table IS NULL"
    // "aggregate:my_aggregate" -> "COMMENT ON AGGREGATE my_aggregate IS NULL"
    
    let parts: Vec<&str> = comment_identifier.split(':').collect();
    
    match parts.as_slice() {
        ["table", name] => Ok(format!("COMMENT ON TABLE {} IS NULL", name)),
        ["view", name] => Ok(format!("COMMENT ON VIEW {} IS NULL", name)),
        ["materialized_view", name] => Ok(format!("COMMENT ON MATERIALIZED VIEW {} IS NULL", name)),
        ["function", name] => {
            // Since pgmg prevents function overloading, we can use the name without parentheses
            let func_name = name.trim_end_matches("()");
            Ok(format!("COMMENT ON FUNCTION {} IS NULL", func_name))
        }
        ["type", name] => Ok(format!("COMMENT ON TYPE {} IS NULL", name)),
        ["domain", name] => Ok(format!("COMMENT ON DOMAIN {} IS NULL", name)),
        ["column", name] => Ok(format!("COMMENT ON COLUMN {} IS NULL", name)),
        ["trigger", trigger_name, table_name] => {
            Ok(format!("COMMENT ON TRIGGER {} ON {} IS NULL", trigger_name, table_name))
        }
        ["aggregate", name] => {
            // Since pgmg prevents aggregate overloading, we can use the name without parentheses
            let agg_name = name.trim_end_matches("()");
            Ok(format!("COMMENT ON AGGREGATE {} IS NULL", agg_name))
        }
        ["procedure", name] => {
            // Since pgmg prevents procedure overloading, we can use the name without parentheses
            let proc_name = name.trim_end_matches("()");
            Ok(format!("COMMENT ON PROCEDURE {} IS NULL", proc_name))
        }
        ["operator", name] => {
            // Operators need their full signature, which should be stored in the comment identifier
            // Format: operator:name(lefttype,righttype)
            Ok(format!("COMMENT ON OPERATOR {} IS NULL", name))
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
        ObjectType::CronJob => "CRON_JOB",  // Will be handled specially
        ObjectType::Aggregate => "AGGREGATE",
        ObjectType::Operator => "OPERATOR",
    };
    
    let full_name = match &qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, qualified_name.name),
        None => qualified_name.name.clone(),
    };
    
    // Note: Triggers are not handled here because DROP TRIGGER requires the table name
    // (e.g., DROP TRIGGER trigger_name ON table_name). Since this function only has
    // access to the object name and type, triggers must be handled specially in the
    // calling code (see apply_drop_for_update and apply_delete_object).
    match object_type {
        ObjectType::Function => {
            // pgmg prevents function overloading, so CASCADE is not needed
            // This ensures safety - unmanaged objects depending on this function will block the drop
            format!("DROP {} IF EXISTS {}", object_type_str, full_name)
        }
        ObjectType::Procedure => {
            // pgmg prevents procedure overloading, so CASCADE is not needed
            // This ensures safety - unmanaged objects depending on this procedure will block the drop
            format!("DROP {} IF EXISTS {}", object_type_str, full_name)
        }
        ObjectType::CronJob => {
            // For cron jobs, we use cron.unschedule
            format!("SELECT cron.unschedule('{}')", qualified_name.name)
        }
        ObjectType::Operator => {
            // Operators need special handling as they require their signature
            // For now, we'll use a simplified approach
            // TODO: Store and retrieve operator signatures properly
            format!("DROP {} IF EXISTS {}", object_type_str, full_name)
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
        ObjectType::CronJob => "cron_job",
        ObjectType::Aggregate => "aggregate",
        ObjectType::Operator => "operator",
    };

    let qualified_name = match &object_name.schema {
        Some(schema) => format!("{}.{}", schema, object_name.name),
        None => object_name.name.clone(),
    };

    client.execute(
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

async fn store_object_dependencies_in_transaction(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    object_name: &crate::sql::QualifiedIdent,
    dependencies: &crate::sql::Dependencies,
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
        ObjectType::CronJob => "cron_job",
        ObjectType::Aggregate => "aggregate",
        ObjectType::Operator => "operator",
    };

    let qualified_name = match &object_name.schema {
        Some(schema) => format!("{}.{}", schema, object_name.name),
        None => object_name.name.clone(),
    };
    
    // First, remove existing dependencies for this object
    client.execute(
        "DELETE FROM pgmg.pgmg_dependencies WHERE dependent_type = $1 AND dependent_name = $2",
        &[&object_type_str, &qualified_name],
    ).await?;
    
    // Store relation dependencies
    for dep in &dependencies.relations {
        let dep_qualified = match &dep.schema {
            Some(schema) => format!("{}.{}", schema, dep.name),
            None => dep.name.clone(),
        };
        // Relations could be tables, views, or materialized views - we store as generic "relation"
        client.execute(
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
        let dep_qualified = match &dep.schema {
            Some(schema) => format!("{}.{}", schema, dep.name),
            None => dep.name.clone(),
        };
        // Determine dependency kind based on dependent object type
        let dep_kind = match object_type {
            ObjectType::Function | ObjectType::Procedure => "soft",
            _ => "hard",
        };
        client.execute(
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
        let dep_qualified = match &dep.schema {
            Some(schema) => format!("{}.{}", schema, dep.name),
            None => dep.name.clone(),
        };
        client.execute(
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
        ObjectType::CronJob => "cron_job",
        ObjectType::Aggregate => "aggregate",
        ObjectType::Operator => "operator",
    };

    let qualified_name = match &object_name.schema {
        Some(schema) => format!("{}.{}", schema, object_name.name),
        None => object_name.name.clone(),
    };

    client.execute(
        "DELETE FROM pgmg.pgmg_state WHERE object_type = $1 AND object_name = $2",
        &[&object_type_str, &qualified_name],
    ).await?;
    
    // Also remove dependencies
    client.execute(
        "DELETE FROM pgmg.pgmg_dependencies WHERE dependent_type = $1 AND dependent_name = $2",
        &[&object_type_str, &qualified_name],
    ).await?;
    
    client.execute(
        "DELETE FROM pgmg.pgmg_dependencies WHERE dependency_type = $1 AND dependency_name = $2",
        &[&object_type_str, &qualified_name],
    ).await?;
    
    // Also remove any comments that reference this object
    match object_type {
        ObjectType::Table | ObjectType::View | ObjectType::MaterializedView | ObjectType::Type => {
            // Remove column comments for this object
            let column_comment_pattern = format!("column:{}.", qualified_name);
            client.execute(
                "DELETE FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name LIKE $1",
                &[&format!("{}%", column_comment_pattern)],
            ).await?;
            
            // Remove the object's own comment
            let object_comment = format!("{}:{}", object_type_str, qualified_name);
            client.execute(
                "DELETE FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name = $1",
                &[&object_comment],
            ).await?;
        }
        ObjectType::Function | ObjectType::Procedure | ObjectType::Operator => {
            // Remove function/procedure/operator comments
            let object_comment = format!("{}:{}", object_type_str, qualified_name);
            client.execute(
                "DELETE FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name = $1",
                &[&object_comment],
            ).await?;
        }
        ObjectType::Trigger => {
            // Triggers have a special format: trigger:trigger_name:table_name
            // When removing a trigger, remove its comment
            // The qualified_name might include schema, so we need to handle both cases
            let trigger_comment_pattern = format!("trigger:{}:", qualified_name);
            
            // Also try without schema prefix in case the comment was stored differently
            let trigger_name_only = qualified_name.split('.').last().unwrap_or(&qualified_name);
            let trigger_comment_pattern_no_schema = format!("trigger:{}:", trigger_name_only);
            
            // Delete using both patterns
            client.execute(
                "DELETE FROM pgmg.pgmg_state WHERE object_type = 'comment' AND (object_name LIKE $1 OR object_name LIKE $2)",
                &[&format!("{}%", trigger_comment_pattern), &format!("{}%", trigger_comment_pattern_no_schema)],
            ).await?;
        }
        _ => {
            // For other object types that might have comments
            let object_comment = format!("{}:{}", object_type_str, qualified_name);
            client.execute(
                "DELETE FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name = $1",
                &[&object_comment],
            ).await?;
        }
    }

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
        ObjectType::CronJob => {
            // Cron jobs are stored in the cron.job table, not in pg_catalog
            return Err("Cron job OID lookup not yet implemented".into());
        }
        ObjectType::Aggregate => {
            "SELECT p.oid FROM pg_proc p 
             JOIN pg_namespace n ON n.oid = p.pronamespace 
             WHERE n.nspname = $1 AND p.proname = $2 AND p.prokind = 'a'"
        }
        ObjectType::Operator => {
            "SELECT o.oid FROM pg_operator o 
             JOIN pg_namespace n ON n.oid = o.oprnamespace 
             WHERE n.nspname = $1 AND o.oprname = $2"
        }
    };
    
    let row = client.query_one(query, &[&schema_name, &object_name]).await?;
    let oid: u32 = row.get(0);
    Ok(oid)
}

/// Print the appropriate success message based on SQL and plpgsql_check results
fn print_apply_success_message(result: &ApplyResult, test_mode: bool) {
    if test_mode {
        return; // Don't print success messages in test mode
    }
    
    if result.plpgsql_errors_found > 0 {
        println!("{}", "Changes applied with errors in PL/pgSQL functions!".red().bold());
        println!("  {} {} PL/pgSQL errors found", "✗".red(), result.plpgsql_errors_found.to_string().red().bold());
        if result.plpgsql_warnings_found > 0 {
            println!("  {} {} PL/pgSQL warnings found", "⚠".yellow(), result.plpgsql_warnings_found.to_string().yellow().bold());
        }
    } else if result.plpgsql_warnings_found > 0 {
        println!("{}", "Changes applied successfully with warnings!".yellow().bold());
        println!("  {} {} PL/pgSQL warnings found", "⚠".yellow(), result.plpgsql_warnings_found.to_string().yellow().bold());
    } else {
        println!("{}", "All changes applied successfully!".green().bold());
    }
}

fn format_object_name(object: &SqlObject) -> String {
    match &object.qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, object.qualified_name.name),
        None => object.qualified_name.name.clone(),
    }
}


async fn get_trigger_table_from_dependencies(
    client: &tokio_postgres::Transaction<'_>,
    trigger_name: &crate::sql::QualifiedIdent,
) -> Result<String, Box<dyn std::error::Error>> {
    let qualified_trigger_name = match &trigger_name.schema {
        Some(schema) => format!("{}.{}", schema, trigger_name.name),
        None => trigger_name.name.clone(),
    };
    
    // Query the dependencies table to find the table this trigger depends on
    let row = client.query_one(
        r#"
        SELECT dependency_name 
        FROM pgmg.pgmg_dependencies 
        WHERE dependent_type = 'trigger' 
          AND dependent_name = $1 
          AND dependency_type = 'relation'
        LIMIT 1
        "#,
        &[&qualified_trigger_name],
    ).await.map_err(|_| format!("Could not find table dependency for trigger {}", qualified_trigger_name))?;
    
    let table_name: String = row.get(0);
    Ok(table_name)
}

async fn get_existing_function_signatures(
    client: &tokio_postgres::Transaction<'_>,
    object_type: &ObjectType,
    qualified_name: &crate::sql::QualifiedIdent,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let (schema_name, function_name) = match &qualified_name.schema {
        Some(s) => (s.as_str(), qualified_name.name.as_str()),
        None => ("public", qualified_name.name.as_str()),
    };
    
    // Handle operators separately as they use pg_operator, not pg_proc
    if object_type == &ObjectType::Operator {
        let query = r#"
            SELECT 
                CASE 
                    WHEN n.nspname = 'public' THEN o.oprname
                    ELSE n.nspname || '.' || o.oprname
                END || '(' || 
                COALESCE(tl.typname, 'NONE') || ', ' || 
                COALESCE(tr.typname, 'NONE') || ')' AS signature
            FROM pg_operator o
            JOIN pg_namespace n ON n.oid = o.oprnamespace
            LEFT JOIN pg_type tl ON tl.oid = o.oprleft
            LEFT JOIN pg_type tr ON tr.oid = o.oprright
            WHERE n.nspname = $1 
              AND o.oprname = $2
        "#;
        
        let rows = client.query(query, &[&schema_name, &function_name]).await?;
        
        let signatures: Vec<String> = rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        
        return Ok(signatures);
    }
    
    let prokind: &str = match object_type {
        ObjectType::Function => "f",
        ObjectType::Procedure => "p", 
        ObjectType::Aggregate => "a",
        _ => return Ok(vec![]),
    };
    
    // Query to get all overloads of a function with their full signatures
    let query = r#"
        SELECT 
            CASE 
                WHEN n.nspname = 'public' THEN p.proname
                ELSE n.nspname || '.' || p.proname
            END || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS signature
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1 
          AND p.proname = $2
          AND p.prokind = $3::char
    "#;
    
    let rows = client.query(query, &[&schema_name, &function_name, &prokind]).await?;
    
    let signatures: Vec<String> = rows.iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    
    Ok(signatures)
}


#[cfg(feature = "cli")]
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
        if result.plpgsql_errors_found > 0 {
            println!("\n{} {} {} {} {}", 
                "✓".yellow().bold(), 
                "Applied".yellow().bold(),
                format!("{} changes", total_changes).yellow(),
                "with".yellow().bold(),
                "PL/pgSQL errors".red().bold()
            );
        } else if result.plpgsql_warnings_found > 0 {
            println!("\n{} {} {} {} {}", 
                "✓".yellow().bold(), 
                "Applied".yellow().bold(),
                format!("{} changes", total_changes).yellow(),
                "with".yellow().bold(),
                "PL/pgSQL warnings".yellow().bold()
            );
        } else {
            println!("\n{} {} {}", 
                "✓".green().bold(), 
                "Successfully applied".green().bold(), 
                format!("{} changes", total_changes).yellow()
            );
        }
    } else {
        println!("\n{} {} {}", 
            "✗".red().bold(), 
            "Apply failed with".red().bold(), 
            format!("{} errors", result.errors.len()).yellow()
        );
    }
    
    // Show plpgsql_check summary if there were any issues
    if result.plpgsql_errors_found > 0 || result.plpgsql_warnings_found > 0 {
        println!();
        println!("{}:", "PL/pgSQL Check Results".bold().yellow());
        if result.plpgsql_errors_found > 0 {
            println!("  {} {} errors found", "✗".red(), result.plpgsql_errors_found.to_string().red().bold());
        }
        if result.plpgsql_warnings_found > 0 {
            println!("  {} {} warnings found", "⚠".yellow(), result.plpgsql_warnings_found.to_string().yellow().bold());
        }
    }
}

// Helper function to quote identifiers properly
fn quote_qualified_identifier(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_identifier(s), quote_identifier(name)),
        None => quote_identifier(name),
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace("\"", "\"\""))
}

/// Check if we're running on AWS RDS by looking for the rdsadmin database
async fn is_aws_rds(client: &tokio_postgres::Client) -> bool {
    match client.query_one(
        "SELECT 1 FROM pg_database WHERE datname = 'rdsadmin'", 
        &[]
    ).await {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Check if a SQL statement should be skipped in test mode
fn should_skip_in_test_mode(sql: &str) -> bool {
    let sql_lower = sql.to_lowercase();
    
    // Skip pg_cron extension creation
    if sql_lower.contains("create extension") && sql_lower.contains("pg_cron") {
        return true;
    }
    
    // Skip comments on pg_cron extension
    if sql_lower.contains("comment on extension") && sql_lower.contains("pg_cron") {
        return true;
    }
    
    // Skip cron.schedule calls
    if sql_lower.contains("cron.schedule") {
        return true;
    }
    
    // Skip cron.unschedule calls
    if sql_lower.contains("cron.unschedule") {
        return true;
    }
    
    false
}

/// Check if a SQL statement is related to plpgsql_check and should be skipped on RDS
fn should_skip_plpgsql_check_on_rds(sql: &str) -> bool {
    let sql_lower = sql.to_lowercase();

    // Skip plpgsql_check extension creation
    if sql_lower.contains("plpgsql_check") {
        return true;
    }

    false
}

/// Helper to order changes by deletion order from dependency graph
fn order_changes_by_deletion<'a>(
    changes: &[&'a ChangeOperation],
    deletion_order: &Option<Vec<ObjectRef>>,
) -> Vec<&'a ChangeOperation> {
    if let Some(del_order) = deletion_order {
        let mut ordered = Vec::new();
        let mut added_indices = std::collections::HashSet::new();

        // Add changes in the order specified by the dependency graph
        for object_ref in del_order {
            for (idx, change) in changes.iter().enumerate() {
                let matches = match change {
                    ChangeOperation::UpdateObject { object, .. } =>
                        object.object_type == object_ref.object_type &&
                        object.qualified_name == object_ref.qualified_name,
                    ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                        let qname = crate::sql::QualifiedIdent::from_qualified_name(object_name);
                        object_type == &object_ref.object_type && qname == object_ref.qualified_name
                    }
                    _ => false,
                };

                if matches && !added_indices.contains(&idx) {
                    ordered.push(*change);
                    added_indices.insert(idx);
                    break;
                }
            }
        }

        // Add any changes not in dependency graph at the end
        for (idx, change) in changes.iter().enumerate() {
            if !added_indices.contains(&idx) {
                ordered.push(*change);
            }
        }

        ordered
    } else {
        // No dependency order available, return as-is
        changes.to_vec()
    }
}

