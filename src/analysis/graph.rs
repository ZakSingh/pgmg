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
}

#[derive(Debug, Clone)]
pub enum DependencyType {
    /// Structural dependency - dependent must be recreated when dependency changes
    /// Examples: views depending on tables, triggers depending on functions
    Hard,
    
    /// Reference dependency - uses runtime lookup, no recreation needed
    /// Examples: functions calling other functions (non-ATOMIC)
    /// These dependencies are tracked for visualization and validation but don't trigger recreation
    Soft,
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
            let object_ref = ObjectRef {
                object_type: obj.object_type.clone(),
                qualified_name: obj.qualified_name.clone(),
            };
            graph.add_node(object_ref);
        }

        // Add edges based on dependencies from parser
        for obj in objects {
            let obj_ref = ObjectRef {
                object_type: obj.object_type.clone(),
                qualified_name: obj.qualified_name.clone(),
            };

            // Filter out built-ins using existing functionality
            let filtered_deps = crate::sql::filter_builtins(
                obj.dependencies.clone(),
                builtin_catalog
            );

            let obj_name = format!("{}.{}",
                obj.qualified_name.schema.as_deref().unwrap_or("public"),
                obj.qualified_name.name
            );

            // Add edges for relation dependencies
            for dep in &filtered_deps.relations {
                // Relations could be tables, views, or materialized views
                if let Some(dep_obj) = objects.iter().find(|o|
                    &o.qualified_name == dep &&
                    matches!(o.object_type, ObjectType::Table | ObjectType::View | ObjectType::MaterializedView)
                ) {
                    let dep_ref = ObjectRef {
                        object_type: dep_obj.object_type.clone(),
                        qualified_name: dep_obj.qualified_name.clone(),
                    };
                    debug!("  Creating edge: {:?} {} -> {:?} {}",
                        dep_ref.object_type,
                        format!("{}.{}", dep_ref.qualified_name.schema.as_deref().unwrap_or("public"), dep_ref.qualified_name.name),
                        obj_ref.object_type,
                        obj_name
                    );
                    graph.add_edge(dep_ref, obj_ref.clone(), DependencyType::Hard)?;
                }
            }
            
            // Add edges for function dependencies
            for dep in &filtered_deps.functions {
                // Function dependencies can match both functions and procedures
                if let Some(dep_obj) = objects.iter().find(|o| 
                    &o.qualified_name == dep && 
                    matches!(o.object_type, ObjectType::Function | ObjectType::Procedure)
                ) {
                    let dep_ref = ObjectRef {
                        object_type: dep_obj.object_type.clone(),
                        qualified_name: dep_obj.qualified_name.clone(),
                    };
                    
                    // Determine dependency type based on the dependent object type
                    let dep_type = match &obj.object_type {
                        // Functions/procedures calling other functions/procedures use runtime lookup
                        ObjectType::Function | ObjectType::Procedure => DependencyType::Soft,
                        // Views, triggers, and other objects have structural dependencies
                        _ => DependencyType::Hard,
                    };
                    
                    graph.add_edge(dep_ref, obj_ref.clone(), dep_type)?;
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
                    let dep_ref = ObjectRef {
                        object_type: dep_obj.object_type.clone(),
                        qualified_name: dep_obj.qualified_name.clone(),
                    };
                    graph.add_edge(dep_ref, obj_ref.clone(), DependencyType::Hard)?;
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

    /// Find all objects that would be affected by changes to the given objects
    /// (i.e., all transitive dependents through HARD dependencies only)
    pub fn affected_by_changes(&self, changed_objects: &[ObjectRef]) -> Vec<ObjectRef> {
        debug!("affected_by_changes called with {} objects", changed_objects.len());
        for obj in changed_objects {
            let obj_name = format!("{}.{}",
                obj.qualified_name.schema.as_deref().unwrap_or("public"),
                obj.qualified_name.name
            );
            debug!("  - {:?} {}", obj.object_type, obj_name);
        }

        let mut affected = std::collections::HashSet::new();
        let mut to_visit: Vec<ObjectRef> = changed_objects.to_vec();

        while let Some(obj_ref) = to_visit.pop() {
            if affected.contains(&obj_ref) {
                continue;
            }

            let obj_name = format!("{}.{}",
                obj_ref.qualified_name.schema.as_deref().unwrap_or("public"),
                obj_ref.qualified_name.name
            );

            affected.insert(obj_ref.clone());

            // Only follow HARD dependencies for recreation
            let dependents = self.hard_dependents_of(&obj_ref);

            for dependent in dependents {
                if !affected.contains(&dependent) {
                    to_visit.push(dependent);
                }
            }
        }

        let result: Vec<ObjectRef> = affected.into_iter()
            .filter(|obj| !changed_objects.contains(obj))
            .collect();

        debug!("affected_by_changes returning {} affected objects", result.len());
        for obj in &result {
            debug!("  - {:?} {}.{}",
                obj.object_type,
                obj.qualified_name.schema.as_deref().unwrap_or("public"),
                obj.qualified_name.name
            );
        }

        result
    }
    
    /// Get dependents of a specific object that have HARD dependencies only
    fn hard_dependents_of(&self, object_ref: &ObjectRef) -> Vec<ObjectRef> {
        if let Some(&node_id) = self.node_map.get(object_ref) {
            // Get all outgoing edges and filter for Hard dependencies
            self.graph.edges_directed(node_id, Direction::Outgoing)
                .filter(|edge| matches!(edge.weight(), DependencyType::Hard))
                .map(|edge| self.graph[edge.target()].clone())
                .collect()
        } else {
            vec![]
        }
    }
    
    /// Get all dependents including soft dependencies (for visualization and checks)
    pub fn all_dependents_of(&self, object_ref: &ObjectRef) -> Vec<ObjectRef> {
        self.dependents_of(object_ref)
    }
    
    /// Get dependents with SOFT dependencies (functions to check but not recreate)
    pub fn soft_dependents_of(&self, object_ref: &ObjectRef) -> Vec<ObjectRef> {
        if let Some(&node_id) = self.node_map.get(object_ref) {
            // Get all outgoing edges and filter for Soft dependencies
            self.graph.edges_directed(node_id, Direction::Outgoing)
                .filter(|edge| matches!(edge.weight(), DependencyType::Soft))
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
            let qualified_name = match &obj_ref.qualified_name.schema {
                Some(schema) => format!("{}.{}", schema, obj_ref.qualified_name.name),
                None => obj_ref.qualified_name.name.clone(),
            };
            
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

                let source_name = match &source_obj.qualified_name.schema {
                    Some(schema) => format!("{}.{}", schema, source_obj.qualified_name.name),
                    None => source_obj.qualified_name.name.clone(),
                };

                let target_name = match &target_obj.qualified_name.schema {
                    Some(schema) => format!("{}.{}", schema, target_obj.qualified_name.name),
                    None => target_obj.qualified_name.name.clone(),
                };
                
                // Create unique node IDs that include object type
                let source_id = format!("{}::{}", format!("{:?}", source_obj.object_type), source_name);
                let target_id = format!("{}::{}", format!("{:?}", target_obj.object_type), target_name);

                let edge_style = match edge_data {
                    DependencyType::Hard => "solid",
                    DependencyType::Soft => "dashed",
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
        }
    }
}

impl From<&SqlObject> for ObjectRef {
    fn from(obj: &SqlObject) -> Self {
        Self {
            object_type: obj.object_type.clone(),
            qualified_name: obj.qualified_name.clone(),
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

        let affected = graph.affected_by_changes(&[users_ref]);
        
        // Should include both user_stats and user_summary (transitively)
        assert_eq!(affected.len(), 2);
        assert!(affected.iter().any(|obj| obj.qualified_name.name == "user_stats"));
        assert!(affected.iter().any(|obj| obj.qualified_name.name == "user_summary"));
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
        let affected = graph.affected_by_changes(&[func1_ref.clone()]);
        
        // Should only include view1 (hard dependency), not func2 (soft dependency)
        assert_eq!(affected.len(), 1);
        assert!(affected.iter().any(|obj| obj.qualified_name.name == "view1"));
        assert!(!affected.iter().any(|obj| obj.qualified_name.name == "func2"));
        
        // Check soft dependents
        let soft_deps = graph.soft_dependents_of(&func1_ref);
        assert_eq!(soft_deps.len(), 1);
        assert!(soft_deps.iter().any(|obj| obj.qualified_name.name == "func2"));
        
        // Check hard dependents  
        let hard_deps = graph.hard_dependents_of(&func1_ref);
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