use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use crate::db::{StateManager, connect_with_url, scan_sql_files, scan_migrations};
use crate::db::state::ConstraintUse;
use crate::sql::{SqlObject, ObjectType, QualifiedIdent, objects::calculate_ddl_hash, MigrationAnalysis, definition_sensitive_to_added_columns, has_late_bound_body, references_only_from_body, AlterImpact, TableAlteration, format_qualified_name, state_object_name, parse_state_object_name, human_object_name, with_or_replace};
use crate::analysis::{DependencyGraph, ObjectRef, Severity, SeverityCounts, severity};
use crate::BuiltinCatalog;
#[cfg(feature = "cli")]
use owo_colors::OwoColorize;
use tracing::{debug, info, warn};

#[derive(Debug, serde::Serialize)]
pub struct PlanResult {
    pub changes: Vec<ChangeOperation>,
    pub new_migrations: Vec<String>,
    #[serde(serialize_with = "serialize_graph_summary")]
    pub dependency_graph: Option<DependencyGraph>,
    pub file_objects: Vec<SqlObject>,
    /// Functions and procedures that read a table a pending migration alters,
    /// drops or renames, but that are left in place rather than recreated.
    /// Their bodies are parsed when they run, so the change can still break
    /// them (a dropped column, an unqualified reference turning ambiguous).
    /// Apply re-validates the SQL-language ones and, when checking is enabled,
    /// re-runs plpgsql_check on the rest.
    #[serde(skip)]
    pub recheck_objects: Vec<SqlObject>,
    /// Worst client-compatibility impact across all changes (Safe when empty).
    pub severity: Severity,
    pub severity_counts: SeverityCounts,
}

/// The petgraph-backed graph can't derive Serialize; emit just the summary
/// counts, which is all consumers of the plan output need.
fn serialize_graph_summary<S: serde::Serializer>(
    graph: &Option<DependencyGraph>,
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;
    match graph {
        None => s.serialize_none(),
        Some(graph) => {
            let mut st = s.serialize_struct("DependencyGraphSummary", 2)?;
            st.serialize_field("node_count", &graph.node_count())?;
            st.serialize_field("edge_count", &graph.edge_count())?;
            st.end()
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChangeOperation {
    CreateObject {
        object: SqlObject,
        reason: String,
        severity: Severity,
    },
    UpdateObject {
        object: SqlObject,
        old_hash: String,
        new_hash: String,
        reason: String,
        severity: Severity,
        /// The object is applied with `CREATE OR REPLACE` and keeps its OID:
        /// it is not dropped, and nothing is recreated on its account except
        /// materialized views and indexes that store its results. Only set for
        /// edited functions and procedures whose interface is unchanged.
        in_place: bool,
    },
    DeleteObject {
        object_type: ObjectType,
        object_name: String,
        reason: String,
        severity: Severity,
    },
    ApplyMigration {
        name: String,
        #[serde(skip)]
        content: String,
        severity: Severity,
    },
}

impl ChangeOperation {
    pub fn severity(&self) -> Severity {
        match self {
            ChangeOperation::CreateObject { severity, .. }
            | ChangeOperation::UpdateObject { severity, .. }
            | ChangeOperation::DeleteObject { severity, .. }
            | ChangeOperation::ApplyMigration { severity, .. } => *severity,
        }
    }

    fn severity_mut(&mut self) -> &mut Severity {
        match self {
            ChangeOperation::CreateObject { severity, .. }
            | ChangeOperation::UpdateObject { severity, .. }
            | ChangeOperation::DeleteObject { severity, .. }
            | ChangeOperation::ApplyMigration { severity, .. } => severity,
        }
    }
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
        recheck_objects: Vec::new(),
        severity: Severity::Safe,
        severity_counts: SeverityCounts::default(),
    };

    // Severities worked out while planning, so the final classification pass
    // does not query the catalog (or warn) a second time for the same object.
    let mut classified_edits: HashMap<ObjectRef, Severity> = HashMap::new();

    // Step 1: Check for new migrations
    if let Some(migrations_dir) = &migrations_dir {
        plan_result.new_migrations = check_new_migrations(
            migrations_dir, 
            &state_manager
        ).await?;
        
        for migration_name in &plan_result.new_migrations {
            // Read migration content for the plan
            let migration_path = migrations_dir.join(format!("{}.sql", migration_name));
            match std::fs::read_to_string(&migration_path) {
                Ok(content) => plan_result.changes.push(ChangeOperation::ApplyMigration {
                    name: migration_name.clone(),
                    content,
                    severity: Severity::Breaking,
                }),
                Err(e) => warn!("Could not read migration {}: {}", migration_path.display(), e),
            }
        }
    }

    // Step 2: Analyze code directory for object changes
    if let Some(code_dir) = &code_dir {
        let file_objects = scan_sql_files(code_dir, &builtin_catalog).await?;
        
        // Check for duplicate object names in files
        validate_no_duplicate_objects_in_files(&file_objects)?;
        
        let db_objects = state_manager.get_tracked_objects().await?;
        
        let mut object_changes = ObjectChanges::new(detect_object_changes(&file_objects, &db_objects).await?);
        let files: HashMap<ObjectRef, &SqlObject> = file_objects.iter()
            .map(|object| (ObjectRef::from(object), object))
            .collect();

        // Edited functions and procedures whose interface is unchanged can be
        // replaced in place rather than dropped. Whether one actually is gets
        // settled once the dependency graph is known (Step 3.5).
        let mut replaceable: Vec<ObjectRef> = Vec::new();
        for change in &object_changes.changes {
            let ChangeOperation::UpdateObject { object, .. } = change else { continue };
            if !matches!(object.object_type, ObjectType::Function | ObjectType::Procedure)
                || with_or_replace(&object.ddl_statement).is_none()
            {
                continue;
            }
            let (classified, can_replace) = severity::classify_routine_edit(&client, object).await;
            classified_edits.insert(ObjectRef::from(object), classified);
            if can_replace {
                replaceable.push(ObjectRef::from(object));
            }
        }

        // Store file objects in the result
        plan_result.file_objects = file_objects.clone();

        // Routines a migration leaves in place although the table they read
        // is being rebuilt around them. See `DependencyGraph::recreation_set`.
        let mut stale_routines: Vec<ObjectRef> = Vec::new();
        // Routines left in place whose body a migration may have broken.
        let mut recheck: Vec<ObjectRef> = Vec::new();
        // Routines whose signature names a table that gains columns. A routine
        // returning the table's row type follows the table, but an object
        // that recorded its result columns when it was created does not.
        let mut widened: Vec<ObjectRef> = Vec::new();

        // Step 2.5: Work out what the pending migrations do to tables and
        // types, and mark the managed objects that must be dropped before the
        // migrations run and recreated afterwards. What that covers depends on
        // what the migrations do; see `MigrationAnalysis` and `AlterImpact`.
        let mut analysis = MigrationAnalysis::default();
        for change in &plan_result.changes {
            let ChangeOperation::ApplyMigration { name, content, .. } = change else { continue };
            match MigrationAnalysis::analyze(content) {
                Ok(migration) => analysis.merge(migration),
                Err(e) => warn!(
                    "Could not parse migration {}; managed objects it affects will not be recreated around it: {}",
                    name, e
                ),
            }
        }

        // Sorted so the plan does not depend on hash iteration order.
        let mut altered: Vec<(&QualifiedIdent, &TableAlteration)> = analysis.alterations.iter().collect();
        altered.sort_by_key(|(table, _)| format_qualified_name(table));

        for (table, alteration) in altered {
            // Dropping a constraint only matters if something depends
            // on the constraint itself, which the catalog knows.
            let mut dropped_constraint_in_use = false;
            for constraint in &alteration.dropped_constraints {
                let in_use = match state_manager.dropped_constraint_use(table, constraint).await? {
                    ConstraintUse::DependedOn | ConstraintUse::UnknownTable => true,
                    ConstraintUse::Unused => false,
                    // Nothing existing depends on a constraint that is not
                    // there, unless an existing one is renamed to that name.
                    ConstraintUse::NoSuchConstraint => analysis.renames_constraints,
                };
                if in_use {
                    debug!("  Constraint {} on {} is depended on (or unknown)",
                        constraint, format_qualified_name(table));
                    dropped_constraint_in_use = true;
                    break;
                }
            }
            let impact = alteration.impact_given(dropped_constraint_in_use);

            debug!("Migrations alter {} ({:?})", format_qualified_name(table), impact);
            if impact == AlterImpact::Unaffected {
                continue;
            }

            let dependents = state_manager
                .find_dependents_of_relations(std::slice::from_ref(table))
                .await?;
            debug!("  {} managed objects depend on it", dependents.len());

            for (obj_type, obj_name) in dependents {
                // A cron job's command is stored as text and resolved when
                // the job runs, so an altered table never requires the job
                // to be rescheduled. Rescheduling would only churn cron.job.
                if obj_type == ObjectType::CronJob {
                    debug!("  Skipping cron job {} (command resolves at run time)", obj_name);
                    continue;
                }
                let obj_ref = state_object_ref(&obj_type, &obj_name);
                let Some(&file_obj) = files.get(&obj_ref) else { continue };

                // A routine that only queries the table from a
                // string body is not bound to it: PostgreSQL never
                // refuses an ALTER on its account and it reads the
                // table's current shape whenever it runs.
                let is_routine = matches!(obj_type, ObjectType::Function | ObjectType::Procedure);
                let late_bound = is_routine
                    && references_only_from_body(&file_obj.ddl_statement, &table.name);

                if is_routine && !late_bound && impact == AlterImpact::ColumnsAdded {
                    widened.push(obj_ref.clone());
                }

                let forced = match impact {
                    AlterImpact::Rebuild if !late_bound => Some("Migration alters dependent table"),
                    AlterImpact::ColumnsAdded
                        if definition_sensitive_to_added_columns(
                            &file_obj.ddl_statement,
                            &alteration.added_columns,
                        ) => Some("Migration adds columns its definition picks up"),
                    _ => None,
                };

                match forced {
                    Some(reason) => object_changes.force_recreation(file_obj, reason, &mut replaceable),
                    // Its file changed too, so it is being updated anyway.
                    None if object_changes.targets(&obj_ref) => {}
                    None => {
                        debug!("  Leaving {:?} {} in place ({:?} does not change its definition)",
                            obj_type, human_name(file_obj), impact);
                        if is_routine {
                            // Left in place, but the change may have
                            // broken its body: re-check it.
                            recheck.push(obj_ref.clone());
                            // What it computes may have changed with
                            // the table, which matters to anything
                            // that stores its results.
                            if impact == AlterImpact::Rebuild {
                                stale_routines.push(obj_ref);
                            }
                        }
                    }
                }
            }

            // A routine can name the table in its signature without querying
            // it (`RETURNS SETOF t` over another function, say).
            if impact == AlterImpact::ColumnsAdded {
                for (obj_type, obj_name) in state_manager.find_dependents_of_types(std::slice::from_ref(table)).await? {
                    let obj_ref = state_object_ref(&obj_type, &obj_name);
                    let Some(&file_obj) = files.get(&obj_ref) else { continue };
                    if matches!(obj_type, ObjectType::Function | ObjectType::Procedure)
                        && !references_only_from_body(&file_obj.ddl_statement, &table.name)
                        && !widened.contains(&obj_ref)
                    {
                        widened.push(obj_ref);
                    }
                }
            }
        }

        // Types the migrations rename, drop or restructure, and tables they
        // drop or rename, take what is bound to them along, whatever tables
        // those objects read: a type named in a signature or used in a body
        // is a binding even for a routine that is otherwise late-bound.
        if !analysis.changed_types.is_empty() {
            let mut type_names: Vec<QualifiedIdent> = analysis.changed_types.iter().cloned().collect();
            type_names.sort_by_key(format_qualified_name);
            let dependents = state_manager.find_dependents_of_types(&type_names).await?;
            debug!("Migrations change {} types; {} managed objects depend on them",
                type_names.len(), dependents.len());

            for (obj_type, obj_name) in dependents {
                if obj_type == ObjectType::CronJob {
                    continue;
                }
                if let Some(&file_obj) = files.get(&state_object_ref(&obj_type, &obj_name)) {
                    object_changes.force_recreation(file_obj, "Migration alters dependent type", &mut replaceable);
                }
            }
        }

        // A table can be dropped, renamed or have a column renamed under a
        // routine that only queries it from a string body. Nothing needs
        // dropping for that, but the body may no longer be valid.
        if !analysis.reshaped_tables.is_empty() {
            let mut tables: Vec<QualifiedIdent> = analysis.reshaped_tables.iter().cloned().collect();
            tables.sort_by_key(format_qualified_name);
            for (obj_type, obj_name) in state_manager.find_dependents_of_relations(&tables).await? {
                let obj_ref = state_object_ref(&obj_type, &obj_name);
                if matches!(obj_type, ObjectType::Function | ObjectType::Procedure) && files.contains_key(&obj_ref) {
                    recheck.push(obj_ref);
                }
            }
        }

        // Step 3: Build dependency graph for affected objects
        if !file_objects.is_empty() || !object_changes.changes.is_empty() {
            // First, identify deleted objects to get their stored dependencies
            let deleted_objects: Vec<(ObjectType, String)> = object_changes.changes.iter()
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

            // Also get stored dependencies for updated objects
            // This is critical for pre-drop ordering: the database still has the OLD dependencies,
            // so we need to consider them when determining drop order
            let updated_objects_for_deps: Vec<(ObjectType, String)> = object_changes.changes.iter()
                .filter_map(|change| match change {
                    ChangeOperation::UpdateObject { object, .. } => {
                        Some((object.object_type.clone(),
                              state_object_name(&object.object_type, &object.qualified_name, object.trigger_table.as_ref())))
                    }
                    _ => None,
                })
                .collect();

            let updated_object_stored_deps = if !updated_objects_for_deps.is_empty() {
                state_manager.get_deleted_object_dependencies(&updated_objects_for_deps).await?
            } else {
                Vec::new()
            };

            // Build a map of stored dependencies for updated objects
            let mut stored_deps_map: HashMap<(ObjectType, String), crate::sql::Dependencies> = HashMap::new();
            for (obj_type, obj_name, obj_trigger_table, deps) in updated_object_stored_deps {
                let key_name = state_object_name(&obj_type, &obj_name, obj_trigger_table.as_ref());
                stored_deps_map.insert((obj_type, key_name), deps);
            }

            // Reconstruct minimal SqlObjects for deleted objects (empty DDL,
            // stored dependencies). Needed as graph nodes in both graphs below.
            let deleted_sql_objects: Vec<SqlObject> = deleted_object_deps.into_iter()
                .map(|(obj_type, obj_name, obj_trigger_table, deps)| {
                    SqlObject::new(
                        obj_type,
                        obj_name,
                        String::new(), // Empty DDL for deleted objects
                        deps,
                        None, // No file path for deleted objects
                    ).with_trigger_table(obj_trigger_table)
                })
                .collect();

            // Build the VALIDATION graph: file objects with their NEW parsed
            // dependencies, plus the deleted objects as nodes. The deleted
            // objects must be present — dependents_of() answers "no dependents"
            // for an object that isn't a node, which would make the
            // deletion-safety check below pass vacuously.
            let mut validation_objects = file_objects.clone();
            validation_objects.extend(deleted_sql_objects.iter().cloned());
            let file_graph = DependencyGraph::build_from_objects(&validation_objects, &builtin_catalog)?;

            // Now build the "merged" graph for PRE-DROP ORDERING
            // This includes stored dependencies so we know what the database currently has
            let mut all_objects_for_ordering = file_objects.clone();

            // Merge stored dependencies into file objects for updated objects
            // This ensures the ordering graph includes both old (database) and new (file) dependencies
            for obj in &mut all_objects_for_ordering {
                let key = (obj.object_type.clone(),
                           state_object_name(&obj.object_type, &obj.qualified_name, obj.trigger_table.as_ref()));
                if let Some(stored_deps) = stored_deps_map.get(&key) {
                    // Merge stored dependencies with parsed dependencies
                    obj.dependencies.relations.extend(stored_deps.relations.clone());
                    obj.dependencies.functions.extend(stored_deps.functions.clone());
                    obj.dependencies.types.extend(stored_deps.types.clone());
                }
            }

            all_objects_for_ordering.extend(deleted_sql_objects);

            // Build ordering graph from merged dependencies
            let graph = DependencyGraph::build_from_objects(&all_objects_for_ordering, &builtin_catalog)?;
            
            // Step 3.25: Validate that deletions are safe
            // Check if any objects being deleted have dependents that aren't also being deleted
            // IMPORTANT: Use file_graph (not graph) for validation - we want to check
            // the NEW dependencies from source files, not the old stored dependencies
            let mut deletion_errors = Vec::new();
            let deleted_object_refs: HashSet<ObjectRef> = object_changes.changes.iter()
                .filter(|change| matches!(change, ChangeOperation::DeleteObject { .. }))
                .filter_map(change_target)
                .collect();

            for deleted_ref in &deleted_object_refs {
                // Use file_graph for validation - this checks if the NEW state is consistent
                let dependents = file_graph.dependents_of(deleted_ref);
                for dependent in dependents {
                    // A dependent that is itself being deleted is fine; one
                    // that still exists in the files is not.
                    if !deleted_object_refs.contains(&dependent) && files.contains_key(&dependent) {
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

            if !deletion_errors.is_empty() {
                return Err(deletion_errors.join("\n").into());
            }

            // Step 3.4: An object parsed at creation that uses a routine
            // whose row type gains columns (`SELECT ... FROM list_items()`
            // where the function returns `SETOF item`) recorded the routine's
            // result columns as they were, star or no star.
            for routine in &widened {
                for dependent in graph.dependents_of(routine) {
                    let Some(&file_obj) = files.get(&dependent) else { continue };
                    if !has_late_bound_body(&file_obj.ddl_statement) {
                        object_changes.force_recreation(
                            file_obj,
                            "Migration adds columns its definition picks up",
                            &mut replaceable,
                        );
                    }
                }
            }

            // Step 3.5: Find all pgmg-managed objects affected by changes
            let updated_objects: Vec<ObjectRef> = object_changes.changes.iter()
                .filter(|change| matches!(change, ChangeOperation::UpdateObject { .. }))
                .filter_map(change_target)
                .collect();

            if !updated_objects.is_empty() || !stale_routines.is_empty() {
                // A replaceable routine stays in place unless it is bound to
                // something that is being dropped (its result type is a view
                // that is recreated, say, or its SQL-standard body uses a view
                // that is deleted), in which case it has to be dropped with
                // it. Dropping it can in turn force others out, so settle by
                // iteration; the set only ever shrinks.
                let affected_objects = loop {
                    let dropped: Vec<ObjectRef> = updated_objects.iter()
                        .filter(|obj| !replaceable.contains(obj))
                        .chain(deleted_object_refs.iter())
                        .cloned()
                        .collect();
                    let stale: Vec<ObjectRef> = replaceable.iter()
                        .chain(stale_routines.iter())
                        .cloned()
                        .collect();
                    let affected = graph.recreation_set(&dropped, &stale);

                    let before = replaceable.len();
                    replaceable.retain(|obj| !affected.contains(obj));
                    if replaceable.len() == before {
                        break affected;
                    }
                };
                debug!("{} pgmg-managed objects affected by changes, {} replaced in place",
                    affected_objects.len(), replaceable.len());

                for change in object_changes.changes.iter_mut() {
                    if let ChangeOperation::UpdateObject { object, in_place, reason, .. } = change {
                        if replaceable.contains(&ObjectRef::from(&*object)) {
                            *in_place = true;
                            *reason = "DDL content has changed; replaced in place".to_string();
                        }
                    }
                }

                // Add dependent objects that need to be recreated
                for affected_ref in affected_objects {
                    if let Some(&file_obj) = files.get(&affected_ref) {
                        object_changes.force_recreation(file_obj, "Dependency requires recreation", &mut replaceable);
                    }
                }
            }

            // An object that is recreated or replaced needs no separate check.
            for obj_ref in recheck {
                if object_changes.targets(&obj_ref) || plan_result.recheck_objects.iter().any(|o| ObjectRef::from(o) == obj_ref) {
                    continue;
                }
                if let Some(&file_obj) = files.get(&obj_ref) {
                    plan_result.recheck_objects.push(file_obj.clone());
                }
            }

            plan_result.changes.extend(object_changes.changes);
            
            // Write graph output if requested
            if let Some(output_path) = output_graph {
                let graphviz_output = graph.to_graphviz();
                std::fs::write(&output_path, graphviz_output)?;
                info!("Dependency graph written to: {:?}", output_path);
            }
            
            plan_result.dependency_graph = Some(graph);
        }
    }

    // Final step: classify the client-compatibility severity of every change.
    // Never fails; anything unclassifiable degrades to Breaking with a warning.
    classify_changes(&client, &mut plan_result, &classified_edits).await;

    Ok(plan_result)
}

/// The managed object a change creates, updates or deletes.
fn change_target(change: &ChangeOperation) -> Option<ObjectRef> {
    match change {
        ChangeOperation::CreateObject { object, .. } | ChangeOperation::UpdateObject { object, .. } => {
            Some(ObjectRef::from(object))
        }
        ChangeOperation::DeleteObject { object_type, object_name, .. } => Some(state_object_ref(object_type, object_name)),
        ChangeOperation::ApplyMigration { .. } => None,
    }
}

/// The object a name in pgmg's state tables refers to.
fn state_object_ref(object_type: &ObjectType, object_name: &str) -> ObjectRef {
    let (qualified_name, trigger_table) = parse_state_object_name(object_type, object_name);
    ObjectRef { object_type: object_type.clone(), qualified_name, trigger_table }
}

fn human_name(object: &SqlObject) -> String {
    human_object_name(&object.object_type, &object.qualified_name, object.trigger_table.as_ref())
}

/// The changes planned for managed objects, with the objects they target
/// indexed so "is this object already part of the plan?" is a lookup.
struct ObjectChanges {
    changes: Vec<ChangeOperation>,
    targeted: HashSet<ObjectRef>,
}

impl ObjectChanges {
    fn new(changes: Vec<ChangeOperation>) -> Self {
        let targeted = changes.iter().filter_map(change_target).collect();
        Self { changes, targeted }
    }

    fn targets(&self, object: &ObjectRef) -> bool {
        self.targeted.contains(object)
    }

    /// Make sure `object` is dropped before the migrations run and recreated
    /// afterwards. An object the plan does not touch yet is scheduled for it;
    /// one that is being updated anyway is dropped already, unless it was
    /// going to be replaced in place, which it now cannot be.
    fn force_recreation(&mut self, object: &SqlObject, reason: &str, replaceable: &mut Vec<ObjectRef>) {
        let object_ref = ObjectRef::from(object);
        if self.targets(&object_ref) {
            replaceable.retain(|candidate| candidate != &object_ref);
            return;
        }
        debug!("  Adding {:?} {} for recreation ({})", object.object_type, human_name(object), reason);
        self.changes.push(ChangeOperation::UpdateObject {
            object: object.clone(),
            old_hash: String::new(),
            new_hash: calculate_ddl_hash(&object.ddl_statement),
            reason: reason.to_string(),
            severity: Severity::Breaking,
            in_place: false,
        });
        self.targeted.insert(object_ref);
    }
}

/// Annotate each change with its client-compatibility severity and roll the
/// worst case (plus counts) up onto the plan. Read-only against the database.
async fn classify_changes(
    client: &tokio_postgres::Client,
    plan: &mut PlanResult,
    classified_edits: &HashMap<ObjectRef, Severity>,
) {
    let mut counts = SeverityCounts::default();
    let mut max = Severity::Safe;
    for change in &mut plan.changes {
        let classified = match &*change {
            ChangeOperation::CreateObject { object, .. } => severity::classify_create(object),
            // Replaced with CREATE OR REPLACE: same OID, same interface.
            ChangeOperation::UpdateObject { in_place: true, .. } => Severity::Safe,
            ChangeOperation::UpdateObject { object, .. } => {
                match classified_edits.get(&ObjectRef::from(object)) {
                    Some(classified) => *classified,
                    None => severity::classify_update(client, object).await,
                }
            }
            ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                severity::classify_delete(object_type, object_name)
            }
            ChangeOperation::ApplyMigration { name, content, .. } => {
                severity::classify_migration(name, content)
            }
        };
        *change.severity_mut() = classified;
    }

    refine_view_severities(client, plan).await;

    for change in &plan.changes {
        counts.record(change.severity());
        max = max.max(change.severity());
    }
    plan.severity = max;
    plan.severity_counts = counts;
}

/// Second opinion for views and materialized views classified `Breaking`.
///
/// The first pass prepares a view's new query against the database as it is,
/// which fails when the query uses a column that this same plan adds. Here the
/// shapes are read off stand-ins that include those additions, so "add a
/// column and expose it in the view" is judged by what the view will really
/// look like. Views whose shape cannot be worked out keep `Breaking`.
async fn refine_view_severities(client: &tokio_postgres::Client, plan: &mut PlanResult) {
    let is_view = |object: &SqlObject| {
        matches!(object.object_type, ObjectType::View | ObjectType::MaterializedView)
    };
    let any_breaking = plan.changes.iter().any(|change| matches!(
        change,
        ChangeOperation::UpdateObject { object, severity: Severity::Breaking, .. } if is_view(object)
    ));
    if !any_breaking {
        return;
    }

    let migrations: Vec<&str> = plan.changes.iter().filter_map(|change| match change {
        ChangeOperation::ApplyMigration { content, .. } => Some(content.as_str()),
        _ => None,
    }).collect();
    let views: Vec<&SqlObject> = plan.changes.iter().filter_map(|change| match change {
        ChangeOperation::CreateObject { object, .. } | ChangeOperation::UpdateObject { object, .. }
            if is_view(object) => Some(object),
        _ => None,
    }).collect();
    let shapes = crate::analysis::shadow::view_shapes_after_apply(client, &migrations, &views).await;

    let mut refined = Vec::new();
    for (index, change) in plan.changes.iter().enumerate() {
        let ChangeOperation::UpdateObject { object, severity: Severity::Breaking, .. } = change else {
            continue;
        };
        if !is_view(object) {
            continue;
        }
        let key = (
            object.qualified_name.schema.clone().unwrap_or_else(|| "public".to_string()),
            object.qualified_name.name.clone(),
        );
        let Some(shape) = shapes.get(&key) else { continue };
        if let Some(severity) = severity::classify_relation_against_shape(client, object, shape).await {
            refined.push((index, severity));
        }
    }
    for (index, severity) in refined {
        *plan.changes[index].severity_mut() = severity;
    }
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
            state_object_name(&db_obj.object_type, &db_obj.object_name, db_obj.trigger_table.as_ref()));
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
            state_object_name(&file_obj.object_type, &file_obj.qualified_name, file_obj.trigger_table.as_ref()));
        file_object_set.insert(key);
    }

    // Check for deleted objects first (in database but not in files)
    for (key, db_obj) in &db_object_map {
        if !file_object_set.contains(key) {
            changes.push(ChangeOperation::DeleteObject {
                object_type: db_obj.object_type.clone(),
                object_name: state_object_name(&db_obj.object_type, &db_obj.object_name, db_obj.trigger_table.as_ref()),
                reason: "Object no longer exists in code".to_string(),
                severity: Severity::Breaking,
            });
        }
    }

    // Check for new or updated objects
    for file_obj in file_objects {
        let key = format!("{:?}:{}", file_obj.object_type,
            state_object_name(&file_obj.object_type, &file_obj.qualified_name, file_obj.trigger_table.as_ref()));

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
                        severity: Severity::Breaking,
                        in_place: false,
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
                    severity: Severity::Breaking,
                });
            }
        }
    }
    
    Ok(changes)
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
        // Check object types that should have unique names within a schema.
        // Triggers are keyed by name:table — same-named triggers on different
        // tables are legitimate, but two on the same table are duplicates.
        // Skip Comments and CronJobs as they are contextual and may be
        // legitimately duplicated.
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
            | ObjectType::Trigger
        );

        if should_check {
            let obj_name = state_object_name(&obj.object_type, &obj.qualified_name, obj.trigger_table.as_ref());
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
                ObjectType::Trigger => "trigger",
                _ => "object",
            };

            // Triggers are keyed as name:table — show that as "name' on table '..."
            let display_name = if locations[0].1 == ObjectType::Trigger {
                let (name, table) = parse_state_object_name(&ObjectType::Trigger, &obj_name);
                human_object_name(&ObjectType::Trigger, &name, table.as_ref())
            } else {
                obj_name.clone()
            };

            let location_list: Vec<String> = locations.iter().map(|(loc, _)| loc.clone()).collect();

            return Err(format!(
                "Multiple definitions of {} '{}' found in SQL files:\n  - {}\n\
                pgmg does not allow duplicate object names. Please rename or remove one definition.",
                object_type_name,
                display_name,
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
                ChangeOperation::UpdateObject { in_place: false, .. } |
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
                            human_object_name(&object.object_type, &object.qualified_name, object.trigger_table.as_ref()).cyan()
                        );
                    }
                    ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                        let (qname, ttable) = parse_state_object_name(object_type, object_name);
                        println!("    {} {} {} (will be deleted)",
                            "↓".red(),
                            format!("{:?}", object_type).to_lowercase().dimmed(),
                            human_object_name(object_type, &qname, ttable.as_ref()).cyan()
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
                ChangeOperation::CreateObject { object, reason, severity } => {
                    // Special handling for comments - display them inline with parent
                    if object.object_type == ObjectType::Comment {
                        // If this comment should be displayed standalone
                        println!("  {} {} {} {} ({}) {}",
                            "+".green().bold(),
                            "CREATE".green().bold(),
                            object.object_type.to_string().yellow(),
                            format_qualified_name(&object.qualified_name).cyan(),
                            reason.dimmed(),
                            severity_tag(*severity)
                        );
                    } else {
                        // Regular object - check if it has an associated comment
                        println!("  {} {} {} {} ({}) {}",
                            "+".green().bold(),
                            "CREATE".green().bold(),
                            object.object_type.to_string().yellow(),
                            human_object_name(&object.object_type, &object.qualified_name, object.trigger_table.as_ref()).cyan(),
                            reason.dimmed(),
                            severity_tag(*severity)
                        );

                        // Look for associated comment in subsequent changes
                        print_associated_comments(plan, i, &mut printed_comments, object);
                    }
                }
                ChangeOperation::UpdateObject { object, old_hash, new_hash, reason, severity, .. } => {
                    // Special handling for comments - display them inline with parent
                    if object.object_type == ObjectType::Comment {
                        // If this comment should be displayed standalone
                        println!("  {} {} {} {} ({}) {}",
                            "~".yellow().bold(),
                            "UPDATE".yellow().bold(),
                            object.object_type.to_string().yellow(),
                            format_qualified_name(&object.qualified_name).cyan(),
                            reason.dimmed(),
                            severity_tag(*severity)
                        );
                    } else {
                        println!("  {} {} {} {} ({}) {}",
                            "~".yellow().bold(),
                            "UPDATE".yellow().bold(),
                            object.object_type.to_string().yellow(),
                            human_object_name(&object.object_type, &object.qualified_name, object.trigger_table.as_ref()).cyan(),
                            reason.dimmed(),
                            severity_tag(*severity)
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
                ChangeOperation::DeleteObject { object_type, object_name, reason, severity } => {
                    let (qname, ttable) = parse_state_object_name(object_type, object_name);
                    println!("  {} {} {} {} ({}) {}",
                        "-".red().bold(),
                        "DELETE".red().bold(),
                        object_type.to_string().yellow(),
                        human_object_name(object_type, &qname, ttable.as_ref()).cyan(),
                        reason.dimmed(),
                        severity_tag(*severity)
                    );
                }
                ChangeOperation::ApplyMigration { name, severity, .. } => {
                    println!("  {} {} {} {}",
                        ">".magenta().bold(),
                        "MIGRATION".magenta().bold(),
                        name.cyan(),
                        severity_tag(*severity)
                    );
                }
            }
        }
    } else if plan.new_migrations.is_empty() {
        println!("\n{}", "No changes detected. Database is up to date.".green());
    }

    if !plan.changes.is_empty() {
        let label = match plan.severity {
            Severity::Safe => "SAFE".green().bold().to_string(),
            Severity::Transient => "TRANSIENT".yellow().bold().to_string(),
            Severity::Breaking => "BREAKING".red().bold().to_string(),
        };
        let counts = &plan.severity_counts;
        println!("\n{}: {} ({} breaking, {} transient, {} safe)",
            "Client compatibility".bold(),
            label,
            counts.breaking,
            counts.transient,
            counts.safe
        );
    }

    if let Some(graph) = &plan.dependency_graph {
        println!("\n{}: {} objects, {} dependencies", 
            "Dependency Graph".bold(),
            graph.node_count().to_string().yellow(),
            graph.edge_count().to_string().yellow()
        );
    }
}

/// Colored `[safe]` / `[transient]` / `[breaking]` tag for a plan line
fn severity_tag(severity: Severity) -> String {
    match severity {
        Severity::Safe => "[safe]".green().to_string(),
        Severity::Transient => "[transient]".yellow().to_string(),
        Severity::Breaking => "[breaking]".red().bold().to_string(),
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

    // Trigger comments are keyed "trigger:<name>:<table>"; accept both the
    // qualified and unqualified spelling of the table.
    let mut expected_names = vec![
        format!("{}:{}", object_type_str, parent_name),
        format!("{}:{}()", object_type_str, parent_name),
    ];
    if parent_object.object_type == ObjectType::Trigger {
        if let Some(table) = &parent_object.trigger_table {
            let table_str = format_qualified_name(table);
            expected_names.clear();
            expected_names.push(format!("trigger:{}:{}", parent_name, table_str));
            if let Some(stripped) = table_str.strip_prefix("public.") {
                expected_names.push(format!("trigger:{}:{}", parent_name, stripped));
            } else if !table_str.contains('.') {
                expected_names.push(format!("trigger:{}:public.{}", parent_name, table_str));
            }
        }
    }

    // Look for comments in subsequent changes
    for (j, change) in plan.changes.iter().enumerate().skip(current_index + 1) {
        if let Some(obj) = get_object_from_change(change) {
            if obj.object_type == ObjectType::Comment {
                // Check if this comment belongs to our parent object
                if expected_names.iter().any(|expected| &obj.qualified_name.name == expected) {
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