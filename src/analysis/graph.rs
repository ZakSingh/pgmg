use std::collections::HashMap;
use petgraph::{Graph, Direction};
use petgraph::graph::NodeIndex;
use crate::sql::{QualifiedIdent, SqlObject, ObjectType};
use crate::builtin_catalog::BuiltinCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub object_type: ObjectType,
    pub qualified_name: QualifiedIdent,
}

#[derive(Debug, Clone)]
pub enum DependencyType {
    Hard,   // Required dependency (e.g., view depends on table)
    Soft,   // Optional dependency (could be missing and still work)
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
            
            // Add edges for all dependencies
            for dep in filtered_deps.relations.iter()
                .chain(filtered_deps.functions.iter())
                .chain(filtered_deps.types.iter()) {
                
                // Find matching object in our set
                if let Some(dep_obj) = objects.iter().find(|o| &o.qualified_name == dep) {
                    let dep_ref = ObjectRef {
                        object_type: dep_obj.object_type.clone(),
                        qualified_name: dep_obj.qualified_name.clone(),
                    };
                    
                    // Add dependency edge (dep_obj -> obj)
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
    /// (i.e., all transitive dependents)
    pub fn affected_by_changes(&self, changed_objects: &[ObjectRef]) -> Vec<ObjectRef> {
        let mut affected = std::collections::HashSet::new();
        let mut to_visit: Vec<ObjectRef> = changed_objects.to_vec();
        
        while let Some(obj_ref) = to_visit.pop() {
            if affected.contains(&obj_ref) {
                continue;
            }
            
            affected.insert(obj_ref.clone());
            
            // Add all direct dependents to visit queue
            for dependent in self.dependents_of(&obj_ref) {
                if !affected.contains(&dependent) {
                    to_visit.push(dependent);
                }
            }
        }
        
        // Remove the originally changed objects from the result
        affected.into_iter()
            .filter(|obj| !changed_objects.contains(obj))
            .collect()
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
                ObjectType::Function => ("lightgreen", "ellipse"), 
                ObjectType::Type => ("lightyellow", "diamond"),
                ObjectType::Domain => ("lightcoral", "hexagon"),
                ObjectType::Index => ("lightgray", "trapezium"),
                ObjectType::Trigger => ("lightpink", "invtriangle"),
            };

            output.push_str(&format!(
                "  \"{}\" [label=\"{}\\n({})\", fillcolor={}, style=\"filled,rounded\", shape={}];\n",
                qualified_name,
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

                let edge_style = match edge_data {
                    DependencyType::Hard => "solid",
                    DependencyType::Soft => "dashed",
                };

                output.push_str(&format!(
                    "  \"{}\" -> \"{}\" [style={}];\n",
                    source_name, target_name, edge_style
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
}