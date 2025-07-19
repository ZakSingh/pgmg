use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use crate::db::{StateManager, connect_with_url, scan_sql_files, scan_migrations};
use crate::sql::{SqlObject, ObjectType, objects::calculate_ddl_hash};
use crate::analysis::{DependencyGraph, ObjectRef};
use crate::BuiltinCatalog;
use owo_colors::OwoColorize;
use tracing::debug;

#[derive(Debug)]
pub struct PlanResult {
    pub changes: Vec<ChangeOperation>,
    pub new_migrations: Vec<String>,
    pub dependency_graph: Option<DependencyGraph>,
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
        
        // Check for duplicate function names in files (no overloading allowed)
        validate_no_function_overloading_in_files(&file_objects)?;
        
        let db_objects = state_manager.get_tracked_objects().await?;
        
        let mut object_changes = detect_object_changes(&file_objects, &db_objects).await?;
        
        // Step 3: Build dependency graph for affected objects
        if !file_objects.is_empty() {
            let graph = DependencyGraph::build_from_objects(&file_objects, &builtin_catalog)?;
            
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
    
    // Check for new or updated objects
    for file_obj in file_objects {
        let key = format!("{:?}:{}", file_obj.object_type,
            format_qualified_name(&file_obj.qualified_name));
        file_object_set.insert(key.clone());
        
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
                        if !existing_funcs.is_empty() {
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
    
    // Check for deleted objects (in database but not in files)
    for (key, db_obj) in &db_object_map {
        if !file_object_set.contains(key) {
            changes.push(ChangeOperation::DeleteObject {
                object_type: db_obj.object_type.clone(),
                object_name: format_qualified_name(&db_obj.object_name),
                reason: "Object no longer exists in code".to_string(),
            });
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

/// Validate that no function names are duplicated in the SQL files
fn validate_no_function_overloading_in_files(file_objects: &[SqlObject]) -> Result<(), Box<dyn std::error::Error>> {
    let mut function_locations: HashMap<String, Vec<String>> = HashMap::new();
    
    // Track all function and procedure names and their locations
    for obj in file_objects {
        if matches!(obj.object_type, ObjectType::Function | ObjectType::Procedure) {
            let func_name = format_qualified_name(&obj.qualified_name);
            let location = obj.source_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "unknown location".to_string());
            
            function_locations.entry(func_name).or_insert_with(Vec::new).push(location);
        }
    }
    
    // Check for duplicates
    for (func_name, locations) in function_locations {
        if locations.len() > 1 {
            return Err(format!(
                "Multiple definitions of function/procedure '{}' found in SQL files:\n  - {}\n\
                pgmg does not support function/procedure overloading. Each function/procedure must have a unique name.",
                func_name,
                locations.join("\n  - ")
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
    }
    
    if !plan.changes.is_empty() {
        println!("\n{}:", "Object Changes".bold());
        for change in &plan.changes {
            match change {
                ChangeOperation::CreateObject { object, reason } => {
                    println!("  {} {} {} {} ({})", 
                        "+".green().bold(),
                        "CREATE".green().bold(),
                        object.object_type.to_string().yellow(),
                        format_qualified_name(&object.qualified_name).cyan(),
                        reason.dimmed()
                    );
                }
                ChangeOperation::UpdateObject { object, old_hash, new_hash, reason } => {
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