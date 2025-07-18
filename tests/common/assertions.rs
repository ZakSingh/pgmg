use pgmg::commands::{PlanResult, ApplyResult, ChangeOperation};
use pgmg::sql::ObjectType;

/// Assert that a plan contains a specific migration
pub fn assert_plan_contains_migration(plan: &PlanResult, migration_name: &str) {
    assert!(
        plan.new_migrations.contains(&migration_name.to_string()),
        "Expected plan to contain migration '{}', but it contains: {:?}",
        migration_name,
        plan.new_migrations
    );
}

/// Assert that a plan contains no changes
pub fn assert_plan_empty(plan: &PlanResult) {
    assert!(
        plan.new_migrations.is_empty() && plan.changes.is_empty(),
        "Expected empty plan, but found {} migrations and {} changes",
        plan.new_migrations.len(),
        plan.changes.len()
    );
}

/// Assert that a plan contains a specific object creation
pub fn assert_plan_contains_create(plan: &PlanResult, object_type: ObjectType, object_name: &str) {
    let found = plan.changes.iter().any(|change| {
        if let ChangeOperation::CreateObject { object, .. } = change {
            object.object_type == object_type && 
            (object.qualified_name.name == object_name || 
             format!("{}.{}", object.qualified_name.schema.as_ref().unwrap_or(&"public".to_string()), object.qualified_name.name) == object_name)
        } else {
            false
        }
    });
    
    assert!(
        found,
        "Expected plan to contain CREATE {:?} '{}', but it doesn't",
        object_type,
        object_name
    );
}

/// Assert that a plan contains a specific object update
pub fn assert_plan_contains_update(plan: &PlanResult, object_type: ObjectType, object_name: &str) {
    let found = plan.changes.iter().any(|change| {
        if let ChangeOperation::UpdateObject { object, .. } = change {
            object.object_type == object_type && 
            (object.qualified_name.name == object_name || 
             format!("{}.{}", object.qualified_name.schema.as_ref().unwrap_or(&"public".to_string()), object.qualified_name.name) == object_name)
        } else {
            false
        }
    });
    
    assert!(
        found,
        "Expected plan to contain UPDATE {:?} '{}', but it doesn't",
        object_type,
        object_name
    );
}

/// Assert that a plan contains a specific object deletion
pub fn assert_plan_contains_delete(plan: &PlanResult, object_type: ObjectType, object_name: &str) {
    let found = plan.changes.iter().any(|change| {
        if let ChangeOperation::DeleteObject { object_type: ot, object_name: on, .. } = change {
            ot == &object_type && on == object_name
        } else {
            false
        }
    });
    
    assert!(
        found,
        "Expected plan to contain DELETE {:?} '{}', but it doesn't",
        object_type,
        object_name
    );
}

/// Assert that apply was successful with no errors
pub fn assert_apply_successful(result: &ApplyResult) {
    assert!(
        result.errors.is_empty(),
        "Expected successful apply, but got {} errors: {:?}",
        result.errors.len(),
        result.errors
    );
}

/// Assert that apply failed with errors
pub fn assert_apply_failed(result: &ApplyResult) {
    assert!(
        !result.errors.is_empty(),
        "Expected apply to fail, but it succeeded"
    );
}

/// Assert that apply applied specific migrations
pub fn assert_migrations_applied(result: &ApplyResult, expected_migrations: &[&str]) {
    for migration in expected_migrations {
        assert!(
            result.migrations_applied.contains(&migration.to_string()),
            "Expected migration '{}' to be applied, but it wasn't. Applied: {:?}",
            migration,
            result.migrations_applied
        );
    }
    
    assert_eq!(
        result.migrations_applied.len(),
        expected_migrations.len(),
        "Expected {} migrations to be applied, but {} were applied",
        expected_migrations.len(),
        result.migrations_applied.len()
    );
}

/// Assert that apply created specific objects
pub fn assert_objects_created(result: &ApplyResult, expected_objects: &[&str]) {
    for object in expected_objects {
        assert!(
            result.objects_created.contains(&object.to_string()),
            "Expected object '{}' to be created, but it wasn't. Created: {:?}",
            object,
            result.objects_created
        );
    }
}

/// Assert that apply updated specific objects
pub fn assert_objects_updated(result: &ApplyResult, expected_objects: &[&str]) {
    for object in expected_objects {
        assert!(
            result.objects_updated.contains(&object.to_string()),
            "Expected object '{}' to be updated, but it wasn't. Updated: {:?}",
            object,
            result.objects_updated
        );
    }
}

/// Assert that apply deleted specific objects
pub fn assert_objects_deleted(result: &ApplyResult, expected_objects: &[&str]) {
    for object in expected_objects {
        assert!(
            result.objects_deleted.contains(&object.to_string()),
            "Expected object '{}' to be deleted, but it wasn't. Deleted: {:?}",
            object,
            result.objects_deleted
        );
    }
}

/// Assert dependency order in plan
pub fn assert_dependency_order(plan: &PlanResult, expected_order: &[&str]) {
    let actual_order: Vec<String> = plan.changes.iter()
        .filter_map(|change| {
            match change {
                ChangeOperation::CreateObject { object, .. } => {
                    Some(object.qualified_name.name.clone())
                }
                ChangeOperation::UpdateObject { object, .. } => {
                    Some(object.qualified_name.name.clone())
                }
                _ => None
            }
        })
        .collect();
    
    for (i, expected) in expected_order.iter().enumerate() {
        if let Some(actual) = actual_order.get(i) {
            assert_eq!(
                actual, expected,
                "Expected object at position {} to be '{}', but found '{}'",
                i, expected, actual
            );
        } else {
            panic!(
                "Expected object '{}' at position {}, but plan only has {} objects",
                expected, i, actual_order.len()
            );
        }
    }
}