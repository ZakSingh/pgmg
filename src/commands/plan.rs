use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use crate::db::{StateManager, connect_with_url, scan_sql_files, scan_migrations};
use crate::sql::{SqlObject, ObjectType, objects::calculate_ddl_hash};
use crate::analysis::{DependencyGraph, ObjectRef};
use crate::BuiltinCatalog;
#[cfg(feature = "cli")]
use owo_colors::OwoColorize;
use tracing::debug;

#[derive(Debug)]
pub struct PlanResult {
    pub changes: Vec<ChangeOperation>,
    pub new_migrations: Vec<String>,
    pub dependency_graph: Option<DependencyGraph>,
    pub file_objects: Vec<SqlObject>,
}

#[derive(Debug, Clone)]
pub enum ChangeOperation {
    CreateObject {
        object: SqlObject,
        reason: String,
    },
    UpdateObject {
        object: SqlObject,
        old_hash: String,
        new_hash: String,
        reason: String,
    },
    DeleteObject {
        object_type: ObjectType,
        object_name: String,
        reason: String,
    },
    ApplyMigration {
        name: String,
        content: String,
    },
}

pub async fn execute_plan(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>, 
    connection_string: String,
    output_graph: Option<PathBuf>,
) -> Result<PlanResult, Box<dyn std::error::Error>> {
    // Connect to database
    let (client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();

    // Initialize state tracking
    let state_manager = StateManager::new(&client);
    state_manager.initialize().await?;

    let builtin_catalog = BuiltinCatalog::from_database(&client).await?;
    
    let mut plan_result = PlanResult {
        changes: Vec::new(),
        new_migrations: Vec::new(),
        dependency_graph: None,
        file_objects: Vec::new(),
    };

    // Step 1: Check for new migrations
    if let Some(migrations_dir) = &migrations_dir {
        plan_result.new_migrations = check_new_migrations(
            migrations_dir, 
            &state_manager
        ).await?;
        
        for migration_name in &plan_result.new_migrations {
            // Read migration content for the plan
            let migration_path = migrations_dir.join(format!("{}.sql", migration_name));
            if let Ok(content) = std::fs::read_to_string(&migration_path) {
                plan_result.changes.push(ChangeOperation::ApplyMigration {
                    name: migration_name.clone(),
                    content,
                });
            }
        }
    }

    // Step 2: Analyze code directory for object changes
    if let Some(code_dir) = &code_dir {
        let file_objects = scan_sql_files(code_dir, &builtin_catalog).await?;
        
        // Check for duplicate object names in files
        validate_no_duplicate_objects_in_files(&file_objects)?;
        
        let db_objects = state_manager.get_tracked_objects().await?;
        
        let mut object_changes = detect_object_changes(&file_objects, &db_objects).await?;
        
        // Store file objects in the result
        plan_result.file_objects = file_objects.clone();
        
        // Step 3: Build dependency graph for affected objects
        if !file_objects.is_empty() || !object_changes.is_empty() {
            // First, identify deleted objects to get their stored dependencies
            let deleted_objects: Vec<(ObjectType, String)> = object_changes.iter()
                .filter_map(|change| match change {
                    ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                        Some((object_type.clone(), object_name.clone()))
                    }
                    _ => None,
                })
                .collect();
            
            // Get stored dependencies for deleted objects
            let deleted_object_deps = if !deleted_objects.is_empty() {
                state_manager.get_deleted_object_dependencies(&deleted_objects).await?
            } else {
                Vec::new()
            };
            
            // Convert deleted objects with dependencies to SqlObjects for graph building
            let mut all_objects_for_graph = file_objects.clone();
            for (obj_type, obj_name, deps) in deleted_object_deps {
                // Create a minimal SqlObject for deleted objects
                let deleted_obj = SqlObject::new(
                    obj_type,
                    obj_name,
                    String::new(), // Empty DDL for deleted objects
                    deps,
                    None, // No file path for deleted objects
                );
                all_objects_for_graph.push(deleted_obj);
            }
            
            // Build graph from both file objects and deleted objects with stored dependencies
            let graph = DependencyGraph::build_from_objects(&all_objects_for_graph, &builtin_catalog)?;
            
            // Step 3.25: Validate that deletions are safe
            // Check if any objects being deleted have dependents that aren't also being deleted
            let mut deletion_errors = Vec::new();
            let deleted_object_refs: HashSet<ObjectRef> = object_changes.iter()
                .filter_map(|change| match change {
                    ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                        let qname = crate::sql::QualifiedIdent::from_qualified_name(object_name);
                        Some(ObjectRef {
                            object_type: object_type.clone(),
                            qualified_name: qname,
                        })
                    }
                    _ => None,
                })
                .collect();
            
            for deleted_ref in &deleted_object_refs {
                let dependents = graph.dependents_of(deleted_ref);
                for dependent in dependents {
                    // Check if this dependent is also being deleted
                    if !deleted_object_refs.contains(&dependent) {
                        // Also check if the dependent exists in the file objects (i.e., it's not being deleted)
                        let dependent_exists_in_files = file_objects.iter().any(|obj| {
                            obj.object_type == dependent.object_type &&
                            obj.qualified_name == dependent.qualified_name
                        });
                        
                        if dependent_exists_in_files {
                            deletion_errors.push(format!(
                                "Cannot delete {} '{}' because {} '{}' depends on it",
                                format!("{:?}", deleted_ref.object_type).to_lowercase(),
                                format_qualified_name(&deleted_ref.qualified_name),
                                format!("{:?}", dependent.object_type).to_lowercase(),
                                format_qualified_name(&dependent.qualified_name),
                            ));
                        }
                    }
                }
            }
            
            if !deletion_errors.is_empty() {
                return Err(deletion_errors.join("\n").into());
            }
            
            // Step 3.5: Find all pgmg-managed objects affected by changes
            let updated_objects: Vec<ObjectRef> = object_changes.iter()
                .filter_map(|change| match change {
                    ChangeOperation::UpdateObject { object, .. } => Some(ObjectRef {
                        object_type: object.object_type.clone(),
                        qualified_name: object.qualified_name.clone(),
                    }),
                    _ => None,
                })
                .collect();
            
            if !updated_objects.is_empty() {
                debug!("Looking for objects affected by {} updates", updated_objects.len());
                for obj in &updated_objects {
                    debug!("  - {:?}: {}", obj.object_type, format_qualified_name(&obj.qualified_name));
                    let deps = graph.dependents_of(obj);
                    debug!("    Direct dependents: {}", deps.len());
                    for dep in &deps {
                        debug!("      -> {:?}: {}", dep.object_type, format_qualified_name(&dep.qualified_name));
                    }
                }
                
                // Use the dependency graph we built from file objects to find affected objects
                let affected_objects = graph.affected_by_changes(&updated_objects);
                debug!("{} pgmg-managed objects affected by changes", affected_objects.len());
                
                // Add dependent objects that need to be recreated
                for affected_ref in affected_objects {
                    // Skip objects that are already in the change list
                    let already_included = object_changes.iter().any(|change| match change {
                        ChangeOperation::UpdateObject { object, .. } |
                        ChangeOperation::CreateObject { object, .. } => {
                            object.object_type == affected_ref.object_type &&
                            object.qualified_name == affected_ref.qualified_name
                        }
                        ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                            let qname = crate::sql::QualifiedIdent::from_qualified_name(object_name);
                            object_type == &affected_ref.object_type &&
                            qname == affected_ref.qualified_name
                        }
                        _ => false,
                    });
                    
                    if !already_included {
                        // Find the object in file_objects to get its DDL
                        if let Some(file_obj) = file_objects.iter().find(|obj| 
                            obj.object_type == affected_ref.object_type &&
                            obj.qualified_name == affected_ref.qualified_name
                        ) {
                            // Add as an update operation (drop and recreate due to dependency)
                            object_changes.push(ChangeOperation::UpdateObject {
                                object: file_obj.clone(),
                                old_hash: String::new(), // We don't have the old hash, but it's not critical
                                new_hash: calculate_ddl_hash(&file_obj.ddl_statement),
                                reason: "Dependency requires recreation".to_string(),
                            });
                        }
                    }
                }
            }
            
            plan_result.changes.extend(object_changes);
            
            // Write graph output if requested
            if let Some(output_path) = output_graph {
                let graphviz_output = graph.to_graphviz();
                std::fs::write(&output_path, graphviz_output)?;
                println!("Dependency graph written to: {:?}", output_path);
            }
            
            plan_result.dependency_graph = Some(graph);
        }
    }

    Ok(plan_result)
}

async fn check_new_migrations(
    migrations_dir: &PathBuf,
    state_manager: &StateManager<'_>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let migration_files = scan_migrations(migrations_dir).await?;
    let applied_migrations = state_manager.get_applied_migration_names().await?;
    
    let mut new_migrations = Vec::new();
    
    for migration_file in migration_files {
        if !applied_migrations.contains(&migration_file.name) {
            new_migrations.push(migration_file.name);
        }
    }
    
    Ok(new_migrations)
}

async fn detect_object_changes(
    file_objects: &[SqlObject],
    db_objects: &[crate::db::ObjectRecord],
) -> Result<Vec<ChangeOperation>, Box<dyn std::error::Error>> {
    let mut changes = Vec::new();
    
    // Create lookup maps
    let mut db_object_map: HashMap<String, &crate::db::ObjectRecord> = HashMap::new();
    let mut db_functions_by_name: HashMap<String, Vec<&crate::db::ObjectRecord>> = HashMap::new();
    
    for db_obj in db_objects {
        let key = format!("{:?}:{}", db_obj.object_type, 
            format_qualified_name(&db_obj.object_name));
        db_object_map.insert(key, db_obj);
        
        // Also track functions and procedures by name only (for overload detection)
        if matches!(db_obj.object_type, ObjectType::Function | ObjectType::Procedure) {
            let func_key = format_qualified_name(&db_obj.object_name);
            db_functions_by_name.entry(func_key).or_insert_with(Vec::new).push(db_obj);
        }
    }
    
    let mut file_object_set: HashSet<String> = HashSet::new();
    
    // Build set of all file objects first
    for file_obj in file_objects {
        let key = format!("{:?}:{}", file_obj.object_type,
            format_qualified_name(&file_obj.qualified_name));
        file_object_set.insert(key);
    }
    
    // Check for deleted objects first (in database but not in files)
    for (key, db_obj) in &db_object_map {
        if !file_object_set.contains(key) {
            changes.push(ChangeOperation::DeleteObject {
                object_type: db_obj.object_type.clone(),
                object_name: format_qualified_name(&db_obj.object_name),
                reason: "Object no longer exists in code".to_string(),
            });
        }
    }
    
    // Check for new or updated objects
    for file_obj in file_objects {
        let key = format!("{:?}:{}", file_obj.object_type,
            format_qualified_name(&file_obj.qualified_name));
        
        let new_hash = calculate_ddl_hash(&file_obj.ddl_statement);
        
        match db_object_map.get(&key) {
            Some(db_obj) => {
                // Object exists in database, check if hash changed
                if db_obj.ddl_hash != new_hash {
                    changes.push(ChangeOperation::UpdateObject {
                        object: file_obj.clone(),
                        old_hash: db_obj.ddl_hash.clone(),
                        new_hash,
                        reason: "DDL content has changed".to_string(),
                    });
                }
            }
            None => {
                // New object - check for function/procedure overloading
                if matches!(file_obj.object_type, ObjectType::Function | ObjectType::Procedure) {
                    let func_name = format_qualified_name(&file_obj.qualified_name);
                    if let Some(existing_funcs) = db_functions_by_name.get(&func_name) {
                        // Check if any non-deleted functions/procedures exist with this name
                        let has_non_deleted_overload = existing_funcs.iter().any(|db_obj| {
                            !changes.iter().any(|change| {
                                matches!(change, ChangeOperation::DeleteObject { object_type, object_name, .. } 
                                    if object_type == &db_obj.object_type && object_name == &format_qualified_name(&db_obj.object_name))
                            })
                        });
                        
                        if has_non_deleted_overload {
                            let obj_type = if file_obj.object_type == ObjectType::Function { "Function" } else { "Procedure" };
                            return Err(format!(
                                "{} '{}' already exists in the database. pgmg does not support function/procedure overloading. \
                                Please use a different name or drop the existing function/procedure first.",
                                obj_type, func_name
                            ).into());
                        }
                    }
                }
                
                changes.push(ChangeOperation::CreateObject {
                    object: file_obj.clone(),
                    reason: "New object not in database".to_string(),
                });
            }
        }
    }
    
    Ok(changes)
}

fn format_qualified_name(qualified_name: &crate::sql::QualifiedIdent) -> String {
    match &qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, qualified_name.name),
        None => qualified_name.name.clone(),
    }
}

/// Parse comment qualified name to extract parent object information
#[allow(dead_code)]
fn parse_comment_parent(comment_name: &str) -> Option<(String, String)> {
    // Comments have format: "object_type:object_name"
    // e.g., "function:api.create_task()", "table:users", "column:users.id"
    if let Some(colon_pos) = comment_name.find(':') {
        let object_type = &comment_name[..colon_pos];
        let object_name = &comment_name[colon_pos + 1..];
        Some((object_type.to_string(), object_name.to_string()))
    } else {
        None
    }
}

/// Extract comment text from DDL statement
fn extract_comment_text(ddl: &str) -> Option<String> {
    // Look for IS 'comment text' pattern
    if let Some(is_pos) = ddl.find(" IS '") {
        let start = is_pos + 5; // Skip " IS '"
        if let Some(end_pos) = ddl[start..].find('\'') {
            return Some(ddl[start..start + end_pos].to_string());
        }
    }
    // Also check for IS "comment text" pattern
    if let Some(is_pos) = ddl.find(" IS \"") {
        let start = is_pos + 5; // Skip " IS ""
        if let Some(end_pos) = ddl[start..].find('"') {
            return Some(ddl[start..start + end_pos].to_string());
        }
    }
    None
}

/// Validate that no object names are duplicated in the SQL files
fn validate_no_duplicate_objects_in_files(file_objects: &[SqlObject]) -> Result<(), Box<dyn std::error::Error>> {
    let mut object_locations: HashMap<String, Vec<(String, ObjectType)>> = HashMap::new();
    
    // Track object names and their locations for types that should be unique
    for obj in file_objects {
        // Check object types that should have unique names within a schema
        // Skip Comments, Triggers, CronJobs as they are contextual and may be legitimately duplicated
        let should_check = matches!(
            obj.object_type,
            ObjectType::Function 
            | ObjectType::Procedure 
            | ObjectType::View 
            | ObjectType::MaterializedView
            | ObjectType::Table 
            | ObjectType::Type 
            | ObjectType::Domain
            | ObjectType::Index
            | ObjectType::Aggregate
        );
        
        if should_check {
            let obj_name = format_qualified_name(&obj.qualified_name);
            let location = match &obj.source_file {
                Some(path) => {
                    match path.strip_prefix(std::env::current_dir().unwrap_or_default()) {
                        Ok(relative_path) => relative_path.display().to_string(),
                        Err(_) => path.display().to_string(),
                    }
                }
                None => "unknown location".to_string(),
            };
            
            // Add line number if available
            let location_with_line = if let Some(line) = obj.start_line {
                format!("{}:{}", location, line)
            } else {
                location
            };
            
            object_locations.entry(obj_name).or_insert_with(Vec::new).push((location_with_line, obj.object_type.clone()));
        }
    }
    
    // Check for duplicates
    for (obj_name, locations) in object_locations {
        if locations.len() > 1 {
            let object_type_name = match locations[0].1 {
                ObjectType::Function => "function",
                ObjectType::Procedure => "procedure", 
                ObjectType::View => "view",
                ObjectType::MaterializedView => "materialized view",
                ObjectType::Table => "table",
                ObjectType::Type => "type",
                ObjectType::Domain => "domain",
                ObjectType::Index => "index",
                ObjectType::Aggregate => "aggregate",
                _ => "object",
            };
            
            let location_list: Vec<String> = locations.iter().map(|(loc, _)| loc.clone()).collect();
            
            return Err(format!(
                "Multiple definitions of {} '{}' found in SQL files:\n  - {}\n\
                pgmg does not allow duplicate object names. Please rename or remove one definition.",
                object_type_name,
                obj_name,
                location_list.join("\n  - ")
            ).into());
        }
    }
    
    Ok(())
}

pub fn print_plan_summary(plan: &PlanResult) {
    println!("\n{}", "=== PGMG Plan Summary ===".bold().blue());
    
    if !plan.new_migrations.is_empty() {
        println!("\n{}:", "New Migrations to Apply".bold());
        for migration in &plan.new_migrations {
            println!("  {} {}", "+".green().bold(), migration.cyan());
        }

        // Show objects that will be pre-dropped before migrations
        let objects_to_predrop: Vec<_> = plan.changes.iter()
            .filter(|c| matches!(c,
                ChangeOperation::UpdateObject { .. } |
                ChangeOperation::DeleteObject { .. }
            ))
            .collect();

        if !objects_to_predrop.is_empty() {
            println!("\n  {}:", "Objects to pre-drop before migrations".dimmed());
            for change in objects_to_predrop {
                match change {
                    ChangeOperation::UpdateObject { object, .. } => {
                        println!("    {} {} {} (will be recreated)",
                            "↓".yellow(),
                            format!("{:?}", object.object_type).to_lowercase().dimmed(),
                            format_qualified_name(&object.qualified_name).cyan()
                        );
                    }
                    ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                        println!("    {} {} {} (will be deleted)",
                            "↓".red(),
                            format!("{:?}", object_type).to_lowercase().dimmed(),
                            object_name.cyan()
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    if !plan.changes.is_empty() {
        println!("\n{}:", "Object Changes".bold());
        
        // Group comments with their parent objects
        let mut printed_comments = HashSet::new();
        
        for (i, change) in plan.changes.iter().enumerate() {
            // Skip if this comment was already printed with its parent
            if let Some(obj) = get_object_from_change(change) {
                if obj.object_type == ObjectType::Comment && printed_comments.contains(&i) {
                    continue;
                }
            }
            
            match change {
                ChangeOperation::CreateObject { object, reason } => {
                    // Special handling for comments - display them inline with parent
                    if object.object_type == ObjectType::Comment {
                        // If this comment should be displayed standalone
                        println!("  {} {} {} {} ({})", 
                            "+".green().bold(),
                            "CREATE".green().bold(),
                            object.object_type.to_string().yellow(),
                            format_qualified_name(&object.qualified_name).cyan(),
                            reason.dimmed()
                        );
                    } else {
                        // Regular object - check if it has an associated comment
                        println!("  {} {} {} {} ({})", 
                            "+".green().bold(),
                            "CREATE".green().bold(),
                            object.object_type.to_string().yellow(),
                            format_qualified_name(&object.qualified_name).cyan(),
                            reason.dimmed()
                        );
                        
                        // Look for associated comment in subsequent changes
                        print_associated_comments(plan, i, &mut printed_comments, object);
                    }
                }
                ChangeOperation::UpdateObject { object, old_hash, new_hash, reason } => {
                    // Special handling for comments - display them inline with parent
                    if object.object_type == ObjectType::Comment {
                        // If this comment should be displayed standalone
                        println!("  {} {} {} {} ({})", 
                            "~".yellow().bold(),
                            "UPDATE".yellow().bold(),
                            object.object_type.to_string().yellow(),
                            format_qualified_name(&object.qualified_name).cyan(),
                            reason.dimmed()
                        );
                    } else {
                        println!("  {} {} {} {} ({})", 
                            "~".yellow().bold(),
                            "UPDATE".yellow().bold(),
                            object.object_type.to_string().yellow(),
                            format_qualified_name(&object.qualified_name).cyan(),
                            reason.dimmed()
                        );
                        if !old_hash.is_empty() && old_hash.len() >= 8 {
                            println!("    {}: {}...", "Old hash".dimmed(), old_hash[..8].to_string().red());
                        }
                        if !new_hash.is_empty() && new_hash.len() >= 8 {
                            println!("    {}: {}...", "New hash".dimmed(), new_hash[..8].to_string().green());
                        }
                        
                        // Look for associated comment in subsequent changes
                        print_associated_comments(plan, i, &mut printed_comments, object);
                    }
                }
                ChangeOperation::DeleteObject { object_type, object_name, reason } => {
                    println!("  {} {} {} {} ({})", 
                        "-".red().bold(),
                        "DELETE".red().bold(),
                        object_type.to_string().yellow(),
                        object_name.cyan(),
                        reason.dimmed()
                    );
                }
                ChangeOperation::ApplyMigration { name, .. } => {
                    println!("  {} {} {}", 
                        ">".magenta().bold(),
                        "MIGRATION".magenta().bold(),
                        name.cyan()
                    );
                }
            }
        }
    } else if plan.new_migrations.is_empty() {
        println!("\n{}", "No changes detected. Database is up to date.".green());
    }
    
    if let Some(graph) = &plan.dependency_graph {
        println!("\n{}: {} objects, {} dependencies", 
            "Dependency Graph".bold(),
            graph.node_count().to_string().yellow(),
            graph.edge_count().to_string().yellow()
        );
    }
}

/// Get object from a change operation
fn get_object_from_change(change: &ChangeOperation) -> Option<&SqlObject> {
    match change {
        ChangeOperation::CreateObject { object, .. } => Some(object),
        ChangeOperation::UpdateObject { object, .. } => Some(object),
        _ => None,
    }
}

/// Print comments associated with an object
fn print_associated_comments(
    plan: &PlanResult, 
    current_index: usize, 
    printed_comments: &mut HashSet<usize>,
    parent_object: &SqlObject
) {
    // Build expected comment name patterns
    let object_type_str = match parent_object.object_type {
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
    
    let parent_name = format_qualified_name(&parent_object.qualified_name);
    let expected_comment_name = format!("{}:{}", object_type_str, parent_name);
    
    // Look for comments in subsequent changes
    for (j, change) in plan.changes.iter().enumerate().skip(current_index + 1) {
        if let Some(obj) = get_object_from_change(change) {
            if obj.object_type == ObjectType::Comment {
                // Check if this comment belongs to our parent object
                if obj.qualified_name.name == expected_comment_name ||
                   obj.qualified_name.name == format!("{}:{}()", object_type_str, parent_name) {
                    // Mark this comment as printed
                    printed_comments.insert(j);
                    
                    // Extract and display the comment text
                    if let Some(comment_text) = extract_comment_text(&obj.ddl_statement) {
                        match change {
                            ChangeOperation::CreateObject { .. } => {
                                println!("    {} {}: {}", 
                                    "└─".dimmed(),
                                    "COMMENT".green().dimmed(),
                                    comment_text.italic()
                                );
                            }
                            ChangeOperation::UpdateObject { .. } => {
                                println!("    {} {}: {}", 
                                    "└─".dimmed(),
                                    "COMMENT".yellow().dimmed(),
                                    comment_text.italic()
                                );
                            }
                            _ => {}
                        }
                    }
                    
                    // Only one comment per object
                    break;
                }
            }
        }
    }
}

/// Quick check for pending changes without building full plan
/// Returns (has_changes, change_count)
pub async fn check_for_pending_changes(
    migrations_dir: Option<PathBuf>,
    code_dir: Option<PathBuf>,
    connection_string: String,
) -> Result<(bool, usize), Box<dyn std::error::Error>> {
    // Connect to database
    let (client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();

    // Initialize state tracking
    let state_manager = StateManager::new(&client);
    state_manager.initialize().await?;

    let mut change_count = 0;

    // Check for new migrations
    if let Some(migrations_dir) = &migrations_dir {
        let new_migrations = check_new_migrations(
            migrations_dir, 
            &state_manager
        ).await?;
        change_count += new_migrations.len();
    }

    // Check for object changes
    if let Some(code_dir) = &code_dir {
        let builtin_catalog = BuiltinCatalog::from_database(&client).await?;
        let file_objects = scan_sql_files(code_dir, &builtin_catalog).await?;
        let db_objects = state_manager.get_tracked_objects().await?;
        let object_changes = detect_object_changes(&file_objects, &db_objects).await?;
        change_count += object_changes.len();
    }

    Ok((change_count > 0, change_count))
}