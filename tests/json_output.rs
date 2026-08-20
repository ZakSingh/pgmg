// Tests for the JSON serialization of PlanResult/ApplyResult used by `--format json`.
// No database required: build results by hand and assert on the serialized shape,
// mirroring the pattern in tests/notify_integration.rs.

use std::path::PathBuf;

use pgmg::commands::{ApplyResult, ChangeOperation, PlanResult};
use pgmg::{Dependencies, DependencyGraph, ObjectType, QualifiedIdent, SqlObject};
use serde_json::Value;

/// Build a PlanResult with dependencies inserted in the given order, so two
/// differently-ordered constructions can be compared for deterministic output.
fn sample_plan_result(relation_order: &[&str]) -> PlanResult {
    let mut deps = Dependencies::default();
    for name in relation_order {
        deps.relations
            .insert(QualifiedIdent::new(Some("public".to_string()), name.to_string()));
    }
    deps.relations
        .insert(QualifiedIdent::new(None, "unqualified".to_string()));

    let object = SqlObject::new(
        ObjectType::MaterializedView,
        QualifiedIdent::new(Some("public".to_string()), "active_users".to_string()),
        "CREATE MATERIALIZED VIEW public.active_users AS SELECT 1".to_string(),
        deps,
        Some(PathBuf::from("sql/views.sql")),
    );

    PlanResult {
        changes: vec![
            ChangeOperation::CreateObject {
                object,
                reason: "New object".to_string(),
            },
            ChangeOperation::DeleteObject {
                object_type: ObjectType::View,
                object_name: "public.old_view".to_string(),
                reason: "Removed from source".to_string(),
            },
            ChangeOperation::ApplyMigration {
                name: "0002_add.sql".to_string(),
                content: "CREATE TABLE t (id int);".to_string(),
            },
        ],
        new_migrations: vec!["0002_add.sql".to_string()],
        dependency_graph: Some(DependencyGraph::new()),
        file_objects: vec![],
    }
}

#[test]
fn plan_result_json_shape() {
    let plan = sample_plan_result(&["zebra", "apple"]);
    let v: Value = serde_json::to_value(&plan).unwrap();

    // Change operations are internally tagged with snake_case kinds
    assert_eq!(v["changes"][0]["type"], "create_object");
    assert_eq!(v["changes"][0]["reason"], "New object");
    assert_eq!(v["changes"][1]["type"], "delete_object");
    assert_eq!(v["changes"][1]["object_type"], "view");
    assert_eq!(v["changes"][1]["object_name"], "public.old_view");
    assert_eq!(v["changes"][2]["type"], "apply_migration");
    assert_eq!(v["changes"][2]["name"], "0002_add.sql");

    // SQL bodies are omitted from JSON output
    assert!(v["changes"][0]["object"].get("ddl_statement").is_none());
    assert!(v["changes"][2].get("content").is_none());

    // ObjectType uses snake_case identifiers
    let object = &v["changes"][0]["object"];
    assert_eq!(object["object_type"], "materialized_view");
    assert_eq!(object["qualified_name"]["schema"], "public");
    assert_eq!(object["qualified_name"]["name"], "active_users");
    assert_eq!(object["source_file"], "sql/views.sql");
    assert!(object["ddl_hash"].as_str().is_some_and(|h| !h.is_empty()));

    // Dependency sets are sorted (schema first, None sorts before Some)
    let relations = object["dependencies"]["relations"].as_array().unwrap();
    let names: Vec<(Option<&str>, &str)> = relations
        .iter()
        .map(|r| (r["schema"].as_str(), r["name"].as_str().unwrap()))
        .collect();
    assert_eq!(
        names,
        vec![
            (None, "unqualified"),
            (Some("public"), "apple"),
            (Some("public"), "zebra"),
        ]
    );

    // The petgraph-backed graph serializes as a summary
    assert_eq!(v["dependency_graph"]["node_count"], 0);
    assert_eq!(v["dependency_graph"]["edge_count"], 0);

    assert_eq!(v["new_migrations"][0], "0002_add.sql");
    assert!(v["file_objects"].as_array().unwrap().is_empty());
}

#[test]
fn plan_result_json_is_deterministic() {
    // Same content, different HashSet insertion order — output must not differ
    let a = serde_json::to_string(&sample_plan_result(&["zebra", "apple", "mango"])).unwrap();
    let b = serde_json::to_string(&sample_plan_result(&["mango", "zebra", "apple"])).unwrap();
    assert_eq!(a, b);
}

#[test]
fn apply_result_json_shape() {
    let result = ApplyResult {
        migrations_applied: vec!["0001_init.sql".to_string()],
        objects_created: vec!["VIEW public.active_users".to_string()],
        objects_updated: vec![],
        objects_deleted: vec![],
        errors: vec![],
        plpgsql_errors_found: 1,
        plpgsql_warnings_found: 2,
    };
    let v: Value = serde_json::to_value(&result).unwrap();

    assert_eq!(v["migrations_applied"][0], "0001_init.sql");
    assert_eq!(v["objects_created"][0], "VIEW public.active_users");
    assert_eq!(v["plpgsql_errors_found"], 1);
    assert_eq!(v["plpgsql_warnings_found"], 2);

    // All seven fields are present, both for a populated and a default result
    for value in [v, serde_json::to_value(ApplyResult::default()).unwrap()] {
        let keys = value.as_object().unwrap();
        for key in [
            "migrations_applied",
            "objects_created",
            "objects_updated",
            "objects_deleted",
            "errors",
            "plpgsql_errors_found",
            "plpgsql_warnings_found",
        ] {
            assert!(keys.contains_key(key), "missing key: {}", key);
        }
    }
}
