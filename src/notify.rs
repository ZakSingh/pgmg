use serde::{Serialize, Deserialize};
use crate::sql::{SqlObject, ObjectType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectLoadedNotification {
    #[serde(rename = "type")]
    pub object_type: String,
    pub schema: Option<String>,
    pub name: String,
    pub oid: Option<u32>,
    pub file: Option<String>,
    pub span: Option<LineSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LineSpan {
    pub start_line: usize,
    pub end_line: usize,
}

impl ObjectLoadedNotification {
    pub fn from_sql_object(obj: &SqlObject) -> Self {
        let object_type = match &obj.object_type {
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
        }.to_string();
        
        let span = match (obj.start_line, obj.end_line) {
            (Some(start), Some(end)) => Some(LineSpan {
                start_line: start,
                end_line: end,
            }),
            _ => None,
        };
        
        Self {
            object_type,
            schema: obj.qualified_name.schema.clone(),
            name: obj.qualified_name.name.clone(),
            oid: None,  // Will be set after object creation
            file: obj.source_file.as_ref().map(|p| p.to_string_lossy().to_string()),
            span,
        }
    }
    
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Emit a NOTIFY event for an object that was loaded
pub async fn emit_object_loaded_notification<C: tokio_postgres::GenericClient>(
    client: &C,
    notification: &ObjectLoadedNotification,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = notification.to_json()?;

    // PostgreSQL NOTIFY has a limit on payload size (8000 bytes)
    // In practice our payloads should be much smaller
    if payload.len() > 7900 {
        return Err("Notification payload too large".into());
    }

    // Use parameterized query to safely handle the payload
    client.execute(
        "SELECT pg_notify($1, $2)",
        &[&"pgmg.object_loaded", &payload],
    ).await?;

    Ok(())
}

/// Channel that pgmg notifies after an apply run successfully changes the schema.
///
/// Clients should `LISTEN "pgmg.apply_succeeded"` and reset any cached statement
/// plans / prepared statements when a message arrives, since the schema may have
/// changed underneath them. Unlike the object-loaded events, this fires in every
/// environment, not just development mode.
pub const APPLY_SUCCEEDED_CHANNEL: &str = "pgmg.apply_succeeded";

/// Summary of what an apply run changed. Sent as the payload of the
/// `pgmg.apply_succeeded` NOTIFY so clients can log/inspect the change.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplySucceededNotification {
    pub migrations_applied: usize,
    pub objects_created: usize,
    pub objects_updated: usize,
    pub objects_deleted: usize,
}

impl ApplySucceededNotification {
    /// True if the apply run made no changes. We skip the NOTIFY in that case
    /// so clients only invalidate caches when the schema actually changed.
    pub fn is_empty(&self) -> bool {
        self.migrations_applied == 0
            && self.objects_created == 0
            && self.objects_updated == 0
            && self.objects_deleted == 0
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Emit a NOTIFY on the `pgmg.apply_succeeded` channel so clients can reset
/// their statement caches after a schema change.
///
/// This is best-effort from the caller's perspective, but when issued inside the
/// apply transaction the NOTIFY is delivered atomically on commit (and discarded
/// on rollback), so listeners never see a change that didn't land.
pub async fn emit_apply_succeeded_notification<C: tokio_postgres::GenericClient>(
    client: &C,
    notification: &ApplySucceededNotification,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = notification.to_json()?;

    // PostgreSQL NOTIFY caps the payload at 8000 bytes; this summary is tiny,
    // but guard anyway to surface a clear error rather than a Postgres failure.
    if payload.len() > 7900 {
        return Err("Notification payload too large".into());
    }

    client.execute(
        "SELECT pg_notify($1, $2)",
        &[&APPLY_SUCCEEDED_CHANNEL, &payload],
    ).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::QualifiedIdent;
    use std::path::PathBuf;
    
    #[test]
    fn test_notification_from_sql_object() {
        let mut obj = SqlObject::new(
            ObjectType::Function,
            QualifiedIdent::new(Some("api".to_string()), "get_user".to_string()),
            "CREATE FUNCTION api.get_user() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;".to_string(),
            Default::default(),
            Some(PathBuf::from("/path/to/functions.sql")),
        );
        obj.start_line = Some(42);
        obj.end_line = Some(45);
        
        let notification = ObjectLoadedNotification::from_sql_object(&obj);
        
        assert_eq!(notification.object_type, "function");
        assert_eq!(notification.schema, Some("api".to_string()));
        assert_eq!(notification.name, "get_user");
        assert_eq!(notification.oid, None);
        assert_eq!(notification.file, Some("/path/to/functions.sql".to_string()));
        assert!(notification.span.is_some());
        
        let span = notification.span.unwrap();
        assert_eq!(span.start_line, 42);
        assert_eq!(span.end_line, 45);
    }
    
    #[test]
    fn test_notification_to_json() {
        let notification = ObjectLoadedNotification {
            object_type: "view".to_string(),
            schema: Some("public".to_string()),
            name: "user_stats".to_string(),
            oid: None,
            file: Some("/sql/views.sql".to_string()),
            span: Some(LineSpan {
                start_line: 10,
                end_line: 15,
            }),
        };
        
        let json = notification.to_json().unwrap();
        assert!(json.contains(r#""type":"view""#));
        assert!(json.contains(r#""schema":"public""#));
        assert!(json.contains(r#""name":"user_stats""#));
        assert!(json.contains(r#""file":"/sql/views.sql""#));
        assert!(json.contains(r#""start_line":10"#));
        assert!(json.contains(r#""end_line":15"#));
    }
    
    #[test]
    fn test_notification_without_optional_fields() {
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
        assert_eq!(notification.oid, None);
        assert_eq!(notification.file, None);
        assert_eq!(notification.span, None);
        
        let json = notification.to_json().unwrap();
        assert!(json.contains(r#""schema":null"#));
        assert!(json.contains(r#""oid":null"#));
        assert!(json.contains(r#""file":null"#));
        assert!(json.contains(r#""span":null"#));
    }

    #[test]
    fn test_apply_succeeded_is_empty() {
        let empty = ApplySucceededNotification {
            migrations_applied: 0,
            objects_created: 0,
            objects_updated: 0,
            objects_deleted: 0,
        };
        assert!(empty.is_empty());

        let changed = ApplySucceededNotification {
            migrations_applied: 0,
            objects_created: 1,
            objects_updated: 0,
            objects_deleted: 0,
        };
        assert!(!changed.is_empty());
    }

    #[test]
    fn test_apply_succeeded_to_json() {
        let notification = ApplySucceededNotification {
            migrations_applied: 2,
            objects_created: 3,
            objects_updated: 1,
            objects_deleted: 0,
        };

        let json = notification.to_json().unwrap();
        assert!(json.contains(r#""migrations_applied":2"#));
        assert!(json.contains(r#""objects_created":3"#));
        assert!(json.contains(r#""objects_updated":1"#));
        assert!(json.contains(r#""objects_deleted":0"#));

        let roundtrip: ApplySucceededNotification = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip, notification);
    }

    #[test]
    fn test_apply_succeeded_channel_name() {
        assert_eq!(APPLY_SUCCEEDED_CHANNEL, "pgmg.apply_succeeded");
    }
}