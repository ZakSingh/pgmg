use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use crate::db::{StateManager, connect_with_url, scan_sql_files, scan_migrations};
use crate::sql::{SqlObject, ObjectType, objects::calculate_ddl_hash};
use crate::analysis::DependencyGraph;
use crate::BuiltinCatalog;

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
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

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
        let db_objects = state_manager.get_tracked_objects().await?;
        
        let object_changes = detect_object_changes(&file_objects, &db_objects).await?;
        plan_result.changes.extend(object_changes);
        
        // Step 3: Build dependency graph for affected objects
        if !file_objects.is_empty() {
            let graph = DependencyGraph::build_from_objects(&file_objects, &builtin_catalog)?;
            
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
    for db_obj in db_objects {
        let key = format!("{:?}:{}", db_obj.object_type, 
            format_qualified_name(&db_obj.object_name));
        db_object_map.insert(key, db_obj);
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
                // New object
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

pub fn print_plan_summary(plan: &PlanResult) {
    println!("\n=== PGMG Plan Summary ===");
    
    if !plan.new_migrations.is_empty() {
        println!("\nNew Migrations to Apply:");
        for migration in &plan.new_migrations {
            println!("  + {}", migration);
        }
    }
    
    if !plan.changes.is_empty() {
        println!("\nObject Changes:");
        for change in &plan.changes {
            match change {
                ChangeOperation::CreateObject { object, reason } => {
                    println!("  + CREATE {} {} ({})", 
                        format!("{:?}", object.object_type).to_uppercase(),
                        format_qualified_name(&object.qualified_name),
                        reason
                    );
                }
                ChangeOperation::UpdateObject { object, old_hash, new_hash, reason } => {
                    println!("  ~ UPDATE {} {} ({})", 
                        format!("{:?}", object.object_type).to_uppercase(),
                        format_qualified_name(&object.qualified_name),
                        reason
                    );
                    println!("    Old hash: {}...", &old_hash[..8]);
                    println!("    New hash: {}...", &new_hash[..8]);
                }
                ChangeOperation::DeleteObject { object_type, object_name, reason } => {
                    println!("  - DELETE {} {} ({})", 
                        format!("{:?}", object_type).to_uppercase(),
                        object_name,
                        reason
                    );
                }
                ChangeOperation::ApplyMigration { name, .. } => {
                    println!("  > MIGRATION {} ", name);
                }
            }
        }
    } else if plan.new_migrations.is_empty() {
        println!("\nNo changes detected. Database is up to date.");
    }
    
    if let Some(graph) = &plan.dependency_graph {
        println!("\nDependency Graph: {} objects, {} dependencies", 
            graph.node_count(), graph.edge_count());
    }
}