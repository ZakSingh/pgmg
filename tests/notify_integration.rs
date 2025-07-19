use pgmg::notify::{ObjectLoadedNotification, LineSpan};
use pgmg::sql::{ObjectType, QualifiedIdent, SqlObject};
use std::path::PathBuf;

#[test]
fn test_notification_json_format() {
    let notification = ObjectLoadedNotification {
        object_type: "function".to_string(),
        schema: Some("api".to_string()),
        name: "get_user".to_string(),
        file: Some("/sql/functions.sql".to_string()),
        span: Some(LineSpan {
            start_line: 42,
            end_line: 45,
        }),
    };
    
    let json = notification.to_json().unwrap();
    
    // Parse back to verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    
    assert_eq!(parsed["type"], "function");
    assert_eq!(parsed["schema"], "api");
    assert_eq!(parsed["name"], "get_user");
    assert_eq!(parsed["file"], "/sql/functions.sql");
    assert_eq!(parsed["span"]["start_line"], 42);
    assert_eq!(parsed["span"]["end_line"], 45);
}

#[test]
fn test_notification_from_sql_object_with_all_fields() {
    let mut obj = SqlObject::new(
        ObjectType::View,
        QualifiedIdent::new(Some("public".to_string()), "user_stats".to_string()),
        "CREATE VIEW public.user_stats AS SELECT COUNT(*) FROM users;".to_string(),
        Default::default(),
        Some(PathBuf::from("/project/sql/views.sql")),
    );
    obj.start_line = Some(10);
    obj.end_line = Some(15);
    
    let notification = ObjectLoadedNotification::from_sql_object(&obj);
    
    assert_eq!(notification.object_type, "view");
    assert_eq!(notification.schema, Some("public".to_string()));
    assert_eq!(notification.name, "user_stats");
    assert_eq!(notification.file, Some("/project/sql/views.sql".to_string()));
    
    let span = notification.span.unwrap();
    assert_eq!(span.start_line, 10);
    assert_eq!(span.end_line, 15);
}

#[test]
fn test_notification_from_sql_object_minimal() {
    let obj = SqlObject::new(
        ObjectType::Table,
        QualifiedIdent::from_name("users".to_string()),
        "CREATE TABLE users (id INT);".to_string(),
        Default::default(),
        None,
    );
    
    let notification = ObjectLoadedNotification::from_sql_object(&obj);
    
    assert_eq!(notification.object_type, "table");
    assert_eq!(notification.schema, None);
    assert_eq!(notification.name, "users");
    assert_eq!(notification.file, None);
    assert_eq!(notification.span, None);
}

#[test]
fn test_all_object_types_mapped_correctly() {
    let test_cases = vec![
        (ObjectType::Table, "table"),
        (ObjectType::View, "view"),
        (ObjectType::MaterializedView, "materialized_view"),
        (ObjectType::Function, "function"),
        (ObjectType::Type, "type"),
        (ObjectType::Domain, "domain"),
        (ObjectType::Index, "index"),
        (ObjectType::Trigger, "trigger"),
    ];
    
    for (obj_type, expected_str) in test_cases {
        let obj = SqlObject::new(
            obj_type,
            QualifiedIdent::from_name("test".to_string()),
            "CREATE ...".to_string(),
            Default::default(),
            None,
        );
        
        let notification = ObjectLoadedNotification::from_sql_object(&obj);
        assert_eq!(notification.object_type, expected_str);
    }
}