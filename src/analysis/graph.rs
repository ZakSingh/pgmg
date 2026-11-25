use std::collections::HashMap;
use petgraph::{Graph, Direction};
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use crate::sql::{QualifiedIdent, SqlObject, ObjectType};
use crate::builtin_catalog::BuiltinCatalog;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub object_type: ObjectType,
    pub qualified_name: QualifiedIdent,
    /// For triggers only: the table the trigger is defined on. Part of node
    /// identity so same-named triggers on different tables stay distinct nodes.
    pub trigger_table: Option<QualifiedIdent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyType {
    /// Structural dependency - the catalog binds the dependent to this object,
    /// so the dependent must be dropped before it and recreated after.
    /// Examples: views depending on tables, triggers depending on functions,
    /// a function whose signature uses a view's row type.
    Hard,

    /// Reference dependency - uses runtime lookup, no recreation needed
    /// Examples: functions calling other functions (non-ATOMIC)
    /// These dependencies are tracked for visualization and validation but don't trigger recreation
    Soft,

    /// A routine that only queries this relation from inside its string body.
    /// Like `Soft`, nothing binds the routine to the relation: it looks the
    /// relation up by name whenever it runs, so it survives the relation being
    /// dropped and recreated and only needs re-checking.
    ///
    /// Unlike `Soft`, what the routine computes follows the relation, so the
    /// edge still carries staleness to objects that store results (see
    /// [`DependencyGraph::recreation_set`]).
    LateBound,
}

#[derive(Debug)]
pub struct DependencyGraph {
    graph: Graph<ObjectRef, DependencyType>,
    node_map: HashMap<ObjectRef, NodeIndex>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            graph: Graph::new(),
            node_map: HashMap::new(),
        }
    }

    /// Build dependency graph from SQL objects using existing parser output
    pub fn build_from_objects(
        objects: &[SqlObject],
        builtin_catalog: &BuiltinCatalog
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut graph = Self::new();

        debug!("Building dependency graph from {} objects", objects.len());

        // Add nodes for each object
        for obj in objects {
            graph.add_node(ObjectRef::from(obj));
        }

        // Add edges based on dependencies from parser
        for obj in objects {
            let obj_ref = ObjectRef::from(obj);

            // Filter out built-ins using existing functionality
            let filtered_deps = crate::sql::filter_builtins(
                obj.dependencies.clone(),
                builtin_catalog
            );

            let obj_name = format!("{}.{}",
                obj.qualified_name.schema.as_deref().unwrap_or("public"),
                obj.qualified_name.name
            );

            // The edge type a structural reference gets when *this* object is
            // the dependent. pg_cron stores a job's command as text and resolves
            // the objects it names when the job runs, so nothing about the job
            // needs recreating when a dependency changes. The edges stay in the
            // graph — deletion validation and create/drop ordering rely on them —
            // but as Soft, so the recreation cascade stops at the job.
            let structural = if obj.object_type == ObjectType::CronJob {
                DependencyType::Soft
            } else {
                DependencyType::Hard
            };

            // For a string-bodied routine, the names its header binds at
            // creation. A relation it reaches only from the body is late-bound.
            let routine_header = if matches!(obj.object_type, ObjectType::Function | ObjectType::Procedure) {
                crate::sql::routine_header_identifiers(&obj.ddl_statement)
            } else {
                None
            };

            let sql_standard_body = routine_header.is_none()
                && matches!(obj.object_type, ObjectType::Function | ObjectType::Procedure)
                && crate::sql::has_sql_standard_body(&obj.ddl_statement);

            // Add edges for relation dependencies
            for dep in &filtered_deps.relations {
                // Relations could be tables, views, or materialized views
                if let Some(dep_obj) = objects.iter().find(|o|
                    &o.qualified_name == dep &&
                    matches!(o.object_type, ObjectType::Table | ObjectType::View | ObjectType::MaterializedView)
                ) {
                    let dep_ref = ObjectRef::from(dep_obj);
                    debug!("  Creating edge: {:?} {} -> {:?} {}",
                        dep_ref.object_type,
                        format!("{}.{}", dep_ref.qualified_name.schema.as_deref().unwrap_or("public"), dep_ref.qualified_name.name),
                        obj_ref.object_type,
                        obj_name
                    );
                    let dep_type = match &routine_header {
                        Some(header) if !header.contains(&dep.name) => DependencyType::LateBound,
                        _ => structural,
                    };
                    graph.add_edge(dep_ref, obj_ref.clone(), dep_type)?;
                }
            }
            
            // Add edges for function dependencies
            for dep in &filtered_deps.functions {
                // Function dependencies can match functions and procedures, and
                // also operators: operator comments record their parent operator
                // in `functions` (Dependencies has no operators field). Operator
                // names are pure symbols, so they cannot collide with function
                // identifiers.
                if let Some(dep_obj) = objects.iter().find(|o|
                    &o.qualified_name == dep &&
                    matches!(o.object_type, ObjectType::Function | ObjectType::Procedure | ObjectType::Operator)
                ) {
                    let dep_ref = ObjectRef::from(dep_obj);

                    // Determine dependency type based on the dependent object type
                    let dep_type = match &obj.object_type {
                        // A SQL-standard body is parsed at creation, so the
                        // catalog ties the routine to every function it calls.
                        ObjectType::Function | ObjectType::Procedure if sql_standard_body => structural,
                        // Functions/procedures calling other functions/procedures use runtime lookup
                        ObjectType::Function | ObjectType::Procedure => DependencyType::Soft,
                        // Views, triggers, and other objects have structural dependencies
                        // (cron jobs excepted; see `structural` above)
                        _ => structural,
                    };
                    
                    graph.add_edge(dep_ref, obj_ref.clone(), dep_type)?;
                }
            }
            
            // Comments on triggers name their parent as `trigger:NAME:TABLE` but
            // record only the trigger's TABLE in `relations` (Dependencies has no
            // trigger bucket). Tie the comment to the trigger itself so it is
            // created after — and dropped before — its parent.
            if obj.object_type == ObjectType::Comment {
                if let Some(rest) = obj.qualified_name.name.strip_prefix("trigger:") {
                    if let Some((trigger_name, table)) = rest.split_once(':') {
                        if let Some(dep_obj) = objects.iter().find(|o|
                            o.object_type == ObjectType::Trigger &&
                            o.qualified_name.name == trigger_name &&
                            o.trigger_table.as_ref()
                                .map(|t| crate::sql::format_qualified_name(t) == table)
                                .unwrap_or(false)
                        ) {
                            let dep_ref = ObjectRef::from(dep_obj);
                            graph.add_edge(dep_ref, obj_ref.clone(), DependencyType::Hard)?;
                        }
                    }
                }
            }

            // Add edges for type dependencies
            for dep in &filtered_deps.types {
                // Type dependencies can be satisfied by types, domains, views, materialized views, or tables
                // (all of these create implicit row types in PostgreSQL)
                if let Some(dep_obj) = objects.iter().find(|o|
                    &o.qualified_name == dep &&
                    matches!(o.object_type, ObjectType::Type | ObjectType::Domain | ObjectType::View | ObjectType::MaterializedView | ObjectType::Table)
                ) {
                    let dep_ref = ObjectRef::from(dep_obj);
                    graph.add_edge(dep_ref, obj_ref.clone(), structural)?;
                }
            }
        }
        
        Ok(graph)
    }

    /// Add a node to the graph
    pub fn add_node(&mut self, object_ref: ObjectRef) -> NodeIndex {
        if let Some(&node_id) = self.node_map.get(&object_ref) {
            node_id
        } else {
            let node_id = self.graph.add_node(object_ref.clone());
            self.node_map.insert(object_ref, node_id);
            node_id
        }
    }

    /// Add an edge between two objects
    pub fn add_edge(
        &mut self, 
        from: ObjectRef, 
        to: ObjectRef, 
        dep_type: DependencyType
    ) -> Result<(), Box<dyn std::error::Error>> {
        let from_node = self.add_node(from);
        let to_node = self.add_node(to);
        
        self.graph.add_edge(from_node, to_node, dep_type);
        Ok(())
    }

    /// Check if the graph has cycles
    pub fn has_cycles(&self) -> bool {
        petgraph::algo::is_cyclic_directed(&self.graph)
    }

    /// Get topologically sorted order for creation (dependencies first)
    pub fn creation_order(&self) -> Result<Vec<ObjectRef>, Box<dyn std::error::Error>> {
        if self.has_cycles() {
            return Err("Dependency graph has cycles".into());
        }

        let sorted_nodes = petgraph::algo::toposort(&self.graph, None)
            .map_err(|_| "Failed to perform topological sort")?;

        Ok(sorted_nodes.into_iter()
            .map(|node_id| self.graph[node_id].clone())
            .collect())
    }

    /// Get reverse topological order for deletion (dependents first)
    pub fn deletion_order(&self) -> Result<Vec<ObjectRef>, Box<dyn std::error::Error>> {
        let mut creation_order = self.creation_order()?;
        creation_order.reverse();
        Ok(creation_order)
    }

    /// Get dependencies of a specific object
    pub fn dependencies_of(&self, object_ref: &ObjectRef) -> Vec<ObjectRef> {
        if let Some(&node_id) = self.node_map.get(object_ref) {
            self.graph.neighbors_directed(node_id, Direction::Incoming)
                .map(|dep_node| self.graph[dep_node].clone())
                .collect()
        } else {
            vec![]
        }
    }

    /// Get dependents of a specific object
    pub fn dependents_of(&self, object_ref: &ObjectRef) -> Vec<ObjectRef> {
        if let Some(&node_id) = self.node_map.get(object_ref) {
            self.graph.neighbors_directed(node_id, Direction::Outgoing)
                .map(|dep_node| self.graph[dep_node].clone())
                .collect()
        } else {
            vec![]
        }
    }

    /// Work out which objects must be dropped and recreated, given
    ///
    /// - `dropped`: objects that are being dropped and recreated, and
    /// - `stale`: objects that stay in place (same OID) but whose behaviour
    ///   may change: a function replaced with `CREATE OR REPLACE`, or a
    ///   late-bound routine reading a table a migration rebuilds.
    ///
    /// Two things force a recreation:
    ///
    /// 1. **Binding.** Whatever the catalog ties to a dropped object goes with
    ///    it: everything reachable from `dropped` over `Hard` edges.
    /// 2. **Stored results.** A materialized view or an index holds values
    ///    computed when it was built. If anything it is derived from changes,
    ///    even something that itself stays in place, those values are out of
    ///    date, so it is rebuilt (and what is bound to it follows by rule 1).
    ///    "Derived from" follows every edge from both `dropped` and `stale`:
    ///    a function that calls a changed function computes something else
    ///    now, just like one that queries a changed view.
    ///
    /// Returns the objects to recreate, excluding `dropped` themselves. A
    /// member of `stale` is returned if it turns out to be bound to something
    /// dropped, in which case it cannot stay in place after all.
    pub fn recreation_set(&self, dropped: &[ObjectRef], stale: &[ObjectRef]) -> Vec<ObjectRef> {
        let changed: Vec<ObjectRef> = dropped.iter().chain(stale).cloned().collect();
        let outdated = self.reachable(&changed, |_| true);

        let mut seeds: Vec<ObjectRef> = dropped.to_vec();
        seeds.extend(outdated.into_iter().filter(|obj| {
            matches!(obj.object_type, ObjectType::MaterializedView | ObjectType::Index)
        }));

        let recreate = self.reachable(&seeds, |dep_type| matches!(dep_type, DependencyType::Hard));

        let result: Vec<ObjectRef> = recreate.into_iter()
            .filter(|obj| !dropped.contains(obj))
            .collect();

        debug!("recreation_set: {} dropped, {} stale -> {} to recreate", dropped.len(), stale.len(), result.len());
        for obj in &result {
            debug!("  - {:?} {}.{}",
                obj.object_type,
                obj.qualified_name.schema.as_deref().unwrap_or("public"),
                obj.qualified_name.name
            );
        }

        result
    }

    /// Every object reachable from `start` (inclusive) over edges whose type
    /// satisfies `follow`.
    fn reachable(
        &self,
        start: &[ObjectRef],
        follow: impl Fn(&DependencyType) -> bool,
    ) -> std::collections::HashSet<ObjectRef> {
        let mut seen = std::collections::HashSet::new();
        let mut to_visit: Vec<ObjectRef> = start.to_vec();

        while let Some(obj_ref) = to_visit.pop() {
            if !seen.insert(obj_ref.clone()) {
                continue;
            }
            if let Some(&node_id) = self.node_map.get(&obj_ref) {
                for edge in self.graph.edges_directed(node_id, Direction::Outgoing) {
                    if follow(edge.weight()) {
                        let target = &self.graph[edge.target()];
                        if !seen.contains(target) {
                            to_visit.push(target.clone());
                        }
                    }
                }
            }
        }

        seen
    }

    /// Get dependents that look this object up at run time (`Soft` and
    /// `LateBound` edges): routines to check but not recreate.
    pub fn soft_dependents_of(&self, object_ref: &ObjectRef) -> Vec<ObjectRef> {
        if let Some(&node_id) = self.node_map.get(object_ref) {
            self.graph.edges_directed(node_id, Direction::Outgoing)
                .filter(|edge| matches!(edge.weight(), DependencyType::Soft | DependencyType::LateBound))
                .map(|edge| self.graph[edge.target()].clone())
                .collect()
        } else {
            vec![]
        }
    }

    /// Get the number of nodes in the graph
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Get the number of edges in the graph
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Output the dependency graph in Graphviz DOT format
    pub fn to_graphviz(&self) -> String {
        let mut output = String::new();
        output.push_str("digraph dependency_graph {\n");
        output.push_str("  rankdir=LR;\n");
        output.push_str("  node [shape=box, style=rounded];\n\n");

        // Add nodes with labels and colors based on object type
        for node_index in self.graph.node_indices() {
            let obj_ref = &self.graph[node_index];
            let qualified_name = obj_ref.display_name();

            let (color, shape) = match obj_ref.object_type {
                ObjectType::Table => ("lightcyan", "rect"),
                ObjectType::View => ("lightblue", "box"),
                ObjectType::MaterializedView => ("darkblue", "box3d"),
                ObjectType::Function => ("lightgreen", "ellipse"),
                ObjectType::Procedure => ("darkgreen", "ellipse"),
                ObjectType::Type => ("lightyellow", "diamond"),
                ObjectType::Domain => ("lightcoral", "hexagon"),
                ObjectType::Index => ("lightgray", "trapezium"),
                ObjectType::Trigger => ("lightpink", "invtriangle"),
                ObjectType::Comment => ("lavender", "note"),
                ObjectType::CronJob => ("orange", "octagon"),
                ObjectType::Aggregate => ("lightsteelblue", "triangle"),
                ObjectType::Operator => ("lightsalmon", "invhouse"),
            };

            // Create unique node ID that includes object type to avoid conflicts
            let node_id = format!("{}::{}", format!("{:?}", obj_ref.object_type), qualified_name);
            
            output.push_str(&format!(
                "  \"{}\" [label=\"{}\\n({})\", fillcolor={}, style=\"filled,rounded\", shape={}];\n",
                node_id,
                qualified_name,
                format!("{:?}", obj_ref.object_type).to_lowercase(),
                color,
                shape
            ));
        }

        output.push_str("\n");

        // Add edges
        for edge_index in self.graph.edge_indices() {
            if let Some((source, target)) = self.graph.edge_endpoints(edge_index) {
                let source_obj = &self.graph[source];
                let target_obj = &self.graph[target];
                let edge_data = &self.graph[edge_index];

                let source_name = source_obj.display_name();
                let target_name = target_obj.display_name();

                // Create unique node IDs that include object type
                let source_id = format!("{}::{}", format!("{:?}", source_obj.object_type), source_name);
                let target_id = format!("{}::{}", format!("{:?}", target_obj.object_type), target_name);

                let edge_style = match edge_data {
                    DependencyType::Hard => "solid",
                    DependencyType::Soft => "dashed",
                    DependencyType::LateBound => "dotted",
                };

                output.push_str(&format!(
                    "  \"{}\" -> \"{}\" [style={}];\n",
                    source_id, target_id, edge_style
                ));
            }
        }

        output.push_str("}\n");
        output
    }
}

impl ObjectRef {
    pub fn new(object_type: ObjectType, qualified_name: QualifiedIdent) -> Self {
        Self {
            object_type,
            qualified_name,
            trigger_table: None,
        }
    }

    /// Display name for graph output; includes the table for triggers so
    /// same-named triggers on different tables get distinct DOT node IDs.
    pub fn display_name(&self) -> String {
        crate::sql::human_object_name(
            &self.object_type,
            &self.qualified_name,
            self.trigger_table.as_ref(),
        )
    }
}

impl From<&SqlObject> for ObjectRef {
    fn from(obj: &SqlObject) -> Self {
        Self {
            object_type: obj.object_type.clone(),
            qualified_name: obj.qualified_name.clone(),
            trigger_table: obj.trigger_table.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{QualifiedIdent, SqlObject, ObjectType, Dependencies};
    use std::collections::HashSet;

    fn create_test_object(
        object_type: ObjectType,
        name: &str,
        schema: Option<&str>,
        dependencies: Dependencies,
    ) -> SqlObject {
        let ddl = format!("CREATE {} {}", 
            match &object_type {
                ObjectType::View => "VIEW",
                ObjectType::MaterializedView => "MATERIALIZED VIEW",
                ObjectType::Function => "FUNCTION", 
                ObjectType::Type => "TYPE",
                _ => "OBJECT",
            },
            name
        );
        
        SqlObject::new(
            object_type,
            QualifiedIdent::new(schema.map(|s| s.to_string()), name.to_string()),
            ddl,
            dependencies,
            None,
        )
    }

    fn deps(relations: &[&str], functions: &[&str]) -> Dependencies {
        Dependencies {
            relations: relations.iter().map(|n| QualifiedIdent::from_name(n.to_string())).collect(),
            functions: functions.iter().map(|n| QualifiedIdent::from_name(n.to_string())).collect(),
            types: HashSet::new(),
        }
    }

    /// pg_cron resolves a job's command text when the job runs, so a changed
    /// dependency must not cascade into the job. The edge itself has to stay:
    /// deletion validation and create/drop ordering are built on it.
    #[test]
    fn test_cron_job_edges_are_soft_but_kept() {
        let objects = vec![
            create_test_object(ObjectType::View, "counts", None, deps(&[], &[])),
            create_test_object(ObjectType::Function, "refresh_counts", None, deps(&["counts"], &[])),
            create_test_object(ObjectType::View, "counts_report", None, deps(&[], &["refresh_counts"])),
            create_test_object(ObjectType::CronJob, "refresh_counts_job", None, deps(&["counts"], &["refresh_counts"])),
        ];
        let graph = DependencyGraph::build_from_objects(&objects, &BuiltinCatalog::new()).unwrap();

        let counts = ObjectRef::new(ObjectType::View, QualifiedIdent::from_name("counts".to_string()));
        let function = ObjectRef::new(ObjectType::Function, QualifiedIdent::from_name("refresh_counts".to_string()));
        let report = ObjectRef::new(ObjectType::View, QualifiedIdent::from_name("counts_report".to_string()));
        let job = ObjectRef::new(ObjectType::CronJob, QualifiedIdent::from_name("refresh_counts_job".to_string()));

        // Changing the function recreates the view that embeds it, not the job.
        let affected = graph.recreation_set(&[function.clone()], &[]);
        assert!(affected.contains(&report));
        assert!(!affected.contains(&job), "cron job must not be recreated for a function change");

        // Changing the relation the job's command names does not reach the job either,
        // even though it does reach the function (and through it the report view).
        let affected = graph.recreation_set(&[counts.clone()], &[]);
        assert!(affected.contains(&function));
        assert!(affected.contains(&report));
        assert!(!affected.contains(&job), "cron job must not be recreated for a relation change");

        // The edges still exist for deletion validation ...
        assert!(graph.dependents_of(&function).contains(&job));
        assert!(graph.dependents_of(&counts).contains(&job));

        // ... and for ordering: the job is scheduled after, and unscheduled before,
        // the objects its command names.
        let creation = graph.creation_order().unwrap();
        let pos = |r: &ObjectRef| creation.iter().position(|o| o == r).unwrap();
        assert!(pos(&function) < pos(&job));
        assert!(pos(&counts) < pos(&job));
    }

    #[test]
    fn test_simple_dependency_graph() {
        let users_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };

        let mut view_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        // View depends on users table
        view_deps.relations.insert(QualifiedIdent::from_name("users".to_string()));

        let objects = vec![
            create_test_object(ObjectType::View, "users", None, users_deps),
            create_test_object(ObjectType::View, "user_stats", None, view_deps),
        ];

        let builtin_catalog = BuiltinCatalog::new(); // Empty catalog for test
        let graph = DependencyGraph::build_from_objects(&objects, &builtin_catalog).unwrap();

        assert_eq!(graph.node_count(), 2);
        assert!(!graph.has_cycles());

        let creation_order = graph.creation_order().unwrap();
        let deletion_order = graph.deletion_order().unwrap();

        // users should come before user_stats in creation order
        let users_pos = creation_order.iter().position(|obj| obj.qualified_name.name == "users").unwrap();
        let stats_pos = creation_order.iter().position(|obj| obj.qualified_name.name == "user_stats").unwrap();
        assert!(users_pos < stats_pos);

        // user_stats should come before users in deletion order  
        let users_pos_del = deletion_order.iter().position(|obj| obj.qualified_name.name == "users").unwrap();
        let stats_pos_del = deletion_order.iter().position(|obj| obj.qualified_name.name == "user_stats").unwrap();
        assert!(stats_pos_del < users_pos_del);
    }

    #[test]
    fn test_affected_by_changes() {
        let table_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(), 
            types: HashSet::new(),
        };

        let mut view1_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        view1_deps.relations.insert(QualifiedIdent::from_name("users".to_string()));

        let mut view2_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        view2_deps.relations.insert(QualifiedIdent::from_name("user_stats".to_string()));

        let objects = vec![
            create_test_object(ObjectType::View, "users", None, table_deps),
            create_test_object(ObjectType::View, "user_stats", None, view1_deps),
            create_test_object(ObjectType::View, "user_summary", None, view2_deps),
        ];

        let builtin_catalog = BuiltinCatalog::new();
        let graph = DependencyGraph::build_from_objects(&objects, &builtin_catalog).unwrap();

        let users_ref = ObjectRef::new(
            ObjectType::View,
            QualifiedIdent::from_name("users".to_string())
        );

        let affected = graph.recreation_set(&[users_ref], &[]);
        
        // Should include both user_stats and user_summary (transitively)
        assert_eq!(affected.len(), 2);
        assert!(affected.iter().any(|obj| obj.qualified_name.name == "user_stats"));
        assert!(affected.iter().any(|obj| obj.qualified_name.name == "user_summary"));
    }

    // ---- late-bound edges and the recreation set ---------------------------

    fn object(object_type: ObjectType, name: &str, ddl: &str, relations: &[&str], functions: &[&str], types: &[&str]) -> SqlObject {
        let idents = |names: &[&str]| names.iter().map(|n| QualifiedIdent::from_name(n.to_string())).collect();
        SqlObject::new(
            object_type,
            QualifiedIdent::from_name(name.to_string()),
            ddl.to_string(),
            Dependencies { relations: idents(relations), functions: idents(functions), types: idents(types) },
            None,
        )
    }

    fn obj_ref(object_type: ObjectType, name: &str) -> ObjectRef {
        ObjectRef::new(object_type, QualifiedIdent::from_name(name.to_string()))
    }

    fn names(objects: Vec<ObjectRef>) -> Vec<String> {
        let mut names: Vec<String> = objects.into_iter().map(|o| o.qualified_name.name).collect();
        names.sort();
        names
    }

    /// orders (view)
    ///   |- count_orders()      queries it from a string body        -> late-bound
    ///   |- list_orders()       returns setof orders                 -> bound
    ///   |- order_ids()         SQL-standard body                    -> bound
    ///   `- order_total (view)  selects from it                      -> bound
    /// count_orders()
    ///   |- order_stats (matview)        stores count_orders()
    ///   |    `- order_stats_idx (index)
    ///   |- order_summary (view)         calls count_orders()
    ///   `- on_order (trigger)           calls nothing here; uses touch()
    fn orders_graph() -> DependencyGraph {
        let objects = vec![
            object(ObjectType::View, "orders", "create view orders as select 1 as id", &[], &[], &[]),
            object(ObjectType::Function, "count_orders",
                "create function count_orders() returns bigint language sql as $$ select count(*) from orders $$",
                &["orders"], &[], &[]),
            object(ObjectType::Function, "list_orders",
                "create function list_orders() returns setof orders language sql as $$ select * from orders $$",
                &["orders"], &[], &["orders"]),
            object(ObjectType::Function, "order_ids",
                "create function order_ids() returns setof int language sql begin atomic select id from orders; end",
                &["orders"], &[], &[]),
            object(ObjectType::View, "order_total", "create view order_total as select count(*) from orders", &["orders"], &[], &[]),
            object(ObjectType::MaterializedView, "order_stats",
                "create materialized view order_stats as select count_orders() as n", &[], &["count_orders"], &[]),
            object(ObjectType::Index, "order_stats_idx", "create index order_stats_idx on order_stats (n)", &["order_stats"], &[], &[]),
            object(ObjectType::View, "order_summary", "create view order_summary as select count_orders() as n", &[], &["count_orders"], &[]),
        ];
        DependencyGraph::build_from_objects(&objects, &BuiltinCatalog::new()).unwrap()
    }

    #[test]
    fn relation_queried_only_from_a_string_body_is_late_bound() {
        let graph = orders_graph();
        let orders = obj_ref(ObjectType::View, "orders");

        // Recreated with the view: what is bound to it. count_orders() is not,
        // but the materialized view storing its results is out of date, and
        // that view's index goes with it.
        assert_eq!(
            names(graph.recreation_set(&[orders.clone()], &[])),
            vec!["list_orders", "order_ids", "order_stats", "order_stats_idx", "order_total"],
        );

        // count_orders() is to be re-checked instead.
        assert_eq!(names(graph.soft_dependents_of(&orders)), vec!["count_orders"]);

        // Every edge is still there for ordering and deletion checks.
        let mut all = names(graph.dependents_of(&orders));
        all.dedup();
        assert_eq!(all, vec!["count_orders", "list_orders", "order_ids", "order_total"]);
        let order = graph.creation_order().unwrap();
        let position = |name: &str| order.iter().position(|o| o.qualified_name.name == name).unwrap();
        assert!(position("orders") < position("count_orders"));
        assert!(position("count_orders") < position("order_stats"));
    }

    #[test]
    fn stale_objects_only_force_out_what_stores_their_results() {
        let graph = orders_graph();
        let count_orders = obj_ref(ObjectType::Function, "count_orders");

        // Dropping the function takes everything bound to it.
        assert_eq!(
            names(graph.recreation_set(&[count_orders.clone()], &[])),
            vec!["order_stats", "order_stats_idx", "order_summary"],
        );

        // Replacing it in place leaves the plain view alone; the materialized
        // view holds values the old body computed, so it is rebuilt.
        assert_eq!(
            names(graph.recreation_set(&[], &[count_orders.clone()])),
            vec!["order_stats", "order_stats_idx"],
        );

        // Nothing changed, nothing to do.
        assert!(graph.recreation_set(&[], &[]).is_empty());
    }

    #[test]
    fn a_stale_object_bound_to_something_dropped_is_returned() {
        let graph = orders_graph();
        let orders = obj_ref(ObjectType::View, "orders");
        let list_orders = obj_ref(ObjectType::Function, "list_orders");
        let count_orders = obj_ref(ObjectType::Function, "count_orders");

        // list_orders() returns the view's row type, so it cannot stay in
        // place while the view is dropped. count_orders() can.
        let recreate = graph.recreation_set(&[orders], &[list_orders.clone(), count_orders.clone()]);
        assert!(recreate.contains(&list_orders));
        assert!(!recreate.contains(&count_orders));
    }

    #[test]
    fn signature_types_and_function_calls_keep_their_edge_types() {
        let objects = vec![
            object(ObjectType::Type, "money_pair", "create type money_pair as (a int, b int)", &[], &[], &[]),
            object(ObjectType::View, "prices", "create view prices as select 1 as id", &[], &[], &[]),
            // A type used only in the body still binds: plpgsql compiles
            // declared variable types into the session's copy of the function.
            object(ObjectType::Function, "uses_type",
                "create function uses_type() returns int language plpgsql as $$ declare p money_pair; begin return 1; end $$",
                &[], &[], &["money_pair"]),
            // A view's row type declared in the body arrives as a type
            // dependency, so it binds too.
            object(ObjectType::Function, "uses_rowtype",
                "create function uses_rowtype() returns int language plpgsql as $$ declare r prices%rowtype; begin return 1; end $$",
                &["prices"], &[], &["prices"]),
            object(ObjectType::Function, "calls", "create function calls() returns int language sql as $$ select uses_type() $$", &[], &["uses_type"], &[]),
            // Procedures are routines too.
            object(ObjectType::Procedure, "purge", "create procedure purge() language plpgsql as $$ begin perform 1 from prices; end $$", &["prices"], &[], &[]),
        ];
        let graph = DependencyGraph::build_from_objects(&objects, &BuiltinCatalog::new()).unwrap();

        assert_eq!(names(graph.recreation_set(&[obj_ref(ObjectType::Type, "money_pair")], &[])), vec!["uses_type"]);
        assert_eq!(names(graph.recreation_set(&[obj_ref(ObjectType::View, "prices")], &[])), vec!["uses_rowtype"]);
        // (uses_rowtype also queries nothing, but its relation edge is
        // late-bound alongside the binding type edge; re-checking it is harmless.)
        assert!(names(graph.soft_dependents_of(&obj_ref(ObjectType::View, "prices"))).contains(&"purge".to_string()));
        assert!(!graph.recreation_set(&[obj_ref(ObjectType::View, "prices")], &[]).contains(&obj_ref(ObjectType::Procedure, "purge")));
        assert_eq!(names(graph.soft_dependents_of(&obj_ref(ObjectType::Function, "uses_type"))), vec!["calls"]);
        assert!(graph.recreation_set(&[obj_ref(ObjectType::Function, "uses_type")], &[]).is_empty());
    }

    #[test]
    fn test_soft_dependencies_not_affected() {
        // Test that function-to-function dependencies don't trigger recreation
        let func1_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };

        let mut func2_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        // func2 calls func1
        func2_deps.functions.insert(QualifiedIdent::from_name("func1".to_string()));

        let mut view_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        // view uses func1
        view_deps.functions.insert(QualifiedIdent::from_name("func1".to_string()));

        let objects = vec![
            create_test_object(ObjectType::Function, "func1", None, func1_deps),
            create_test_object(ObjectType::Function, "func2", None, func2_deps),
            create_test_object(ObjectType::View, "view1", None, view_deps),
        ];

        let builtin_catalog = BuiltinCatalog::new();
        let graph = DependencyGraph::build_from_objects(&objects, &builtin_catalog).unwrap();

        let func1_ref = ObjectRef::new(
            ObjectType::Function,
            QualifiedIdent::from_name("func1".to_string())
        );

        // Check affected by changes (only hard dependencies)
        let affected = graph.recreation_set(&[func1_ref.clone()], &[]);
        
        // Should only include view1 (hard dependency), not func2 (soft dependency)
        assert_eq!(affected.len(), 1);
        assert!(affected.iter().any(|obj| obj.qualified_name.name == "view1"));
        assert!(!affected.iter().any(|obj| obj.qualified_name.name == "func2"));
        
        // Check soft dependents
        let soft_deps = graph.soft_dependents_of(&func1_ref);
        assert_eq!(soft_deps.len(), 1);
        assert!(soft_deps.iter().any(|obj| obj.qualified_name.name == "func2"));
        
        // Check hard dependents
        let hard_deps = graph.recreation_set(&[func1_ref.clone()], &[]);
        assert_eq!(hard_deps.len(), 1);
        assert!(hard_deps.iter().any(|obj| obj.qualified_name.name == "view1"));
    }

    #[test]
    fn test_graphviz_output() {
        let users_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };

        let mut view_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        view_deps.relations.insert(QualifiedIdent::from_name("users".to_string()));

        let objects = vec![
            create_test_object(ObjectType::View, "users", None, users_deps),
            create_test_object(ObjectType::Function, "user_stats", Some("api"), view_deps),
        ];

        let builtin_catalog = BuiltinCatalog::new();
        let graph = DependencyGraph::build_from_objects(&objects, &builtin_catalog).unwrap();

        let graphviz_output = graph.to_graphviz();
        
        // Check that the output contains expected elements
        assert!(graphviz_output.contains("digraph dependency_graph"));
        assert!(graphviz_output.contains("users"));
        assert!(graphviz_output.contains("api.user_stats"));
        assert!(graphviz_output.contains("lightblue")); // View color
        assert!(graphviz_output.contains("lightgreen")); // Function color
        assert!(graphviz_output.contains("->"));  // Edge indicator
    }

    #[test]
    fn test_composite_type_depends_on_materialized_view() {
        // Test that a composite type with a type dependency on a materialized view
        // is correctly tracked in the dependency graph
        let mv_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };

        let mut type_deps = Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        };
        // Composite type depends on materialized view's implicit row type
        type_deps.types.insert(QualifiedIdent::new(Some("core".to_string()), "seller_stats".to_string()));

        let objects = vec![
            create_test_object(ObjectType::MaterializedView, "seller_stats", Some("core"), mv_deps),
            create_test_object(ObjectType::Type, "seller_feedback_summary", Some("api"), type_deps),
        ];

        let builtin_catalog = BuiltinCatalog::new();
        let graph = DependencyGraph::build_from_objects(&objects, &builtin_catalog).unwrap();

        // Verify the dependency edge was created
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        assert!(!graph.has_cycles());

        let mv_ref = ObjectRef::new(
            ObjectType::MaterializedView,
            QualifiedIdent::new(Some("core".to_string()), "seller_stats".to_string())
        );
        let type_ref = ObjectRef::new(
            ObjectType::Type,
            QualifiedIdent::new(Some("api".to_string()), "seller_feedback_summary".to_string())
        );

        // Verify the composite type is a dependent of the materialized view
        let dependents = graph.dependents_of(&mv_ref);
        assert_eq!(dependents.len(), 1);
        assert_eq!(dependents[0].qualified_name.name, "seller_feedback_summary");

        // Verify the materialized view is a dependency of the composite type
        let dependencies = graph.dependencies_of(&type_ref);
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0].qualified_name.name, "seller_stats");

        // Verify deletion order: composite type should come before materialized view
        let deletion_order = graph.deletion_order().unwrap();
        let type_pos = deletion_order.iter().position(|obj| obj.qualified_name.name == "seller_feedback_summary").unwrap();
        let mv_pos = deletion_order.iter().position(|obj| obj.qualified_name.name == "seller_stats").unwrap();
        assert!(type_pos < mv_pos, "Composite type should be deleted before materialized view");

        // Verify creation order: materialized view should come before composite type
        let creation_order = graph.creation_order().unwrap();
        let type_pos_create = creation_order.iter().position(|obj| obj.qualified_name.name == "seller_feedback_summary").unwrap();
        let mv_pos_create = creation_order.iter().position(|obj| obj.qualified_name.name == "seller_stats").unwrap();
        assert!(mv_pos_create < type_pos_create, "Materialized view should be created before composite type");
    }
}