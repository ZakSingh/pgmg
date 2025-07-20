use pgmg::commands::plan::{PlanResult, ChangeOperation, print_plan_summary};
use pgmg::sql::ObjectType;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

/// A writer that captures output into a string
pub struct CaptureWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CaptureWriter {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    pub fn get_output(&self) -> String {
        let buffer = self.buffer.lock().unwrap();
        String::from_utf8_lossy(&buffer).to_string()
    }
}

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.extend_from_slice(buf);
        Ok(buf.len())
    }
    
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Capture the output of print_plan_summary
/// Note: This currently captures stdout, so it may not work perfectly in tests
/// A better approach would be to modify print_plan_summary to accept a writer
pub fn capture_plan_output(plan: &PlanResult) -> String {
    // For now, we'll just call print_plan_summary normally
    // In a real implementation, we'd modify the function to accept a writer
    print_plan_summary(plan);
    
    // Return empty string as we can't easily capture stdout in tests
    // The actual tests will need to verify the plan structure instead
    String::new()
}

/// Assert that a comment is properly associated with its parent object in the plan
pub fn assert_comment_grouped_with_parent(
    plan: &PlanResult,
    parent_type: ObjectType,
    parent_name: &str,
    comment_text: &str,
) -> Result<(), String> {
    // Find the parent object in the plan
    let mut parent_index = None;
    let mut comment_index = None;
    
    for (i, change) in plan.changes.iter().enumerate() {
        match change {
            ChangeOperation::CreateObject { object, .. } |
            ChangeOperation::UpdateObject { object, .. } => {
                if object.object_type == parent_type && 
                   format_object_name(object) == parent_name {
                    parent_index = Some(i);
                }
                
                if object.object_type == ObjectType::Comment &&
                   object.ddl_statement.contains(comment_text) {
                    comment_index = Some(i);
                }
            }
            _ => {}
        }
    }
    
    match (parent_index, comment_index) {
        (Some(p_idx), Some(c_idx)) => {
            if c_idx > p_idx {
                // Comment comes after parent, which is correct for grouping
                Ok(())
            } else {
                Err(format!(
                    "Comment (index {}) should come after parent object (index {})",
                    c_idx, p_idx
                ))
            }
        }
        (None, _) => Err(format!("Parent object {} {} not found in plan", parent_type, parent_name)),
        (_, None) => Err(format!("Comment with text '{}' not found in plan", comment_text)),
    }
}

/// Helper to format object name with schema
fn format_object_name(object: &pgmg::sql::SqlObject) -> String {
    match &object.qualified_name.schema {
        Some(schema) => format!("{}.{}", schema, object.qualified_name.name),
        None => object.qualified_name.name.clone(),
    }
}

/// Verify that the comment naming follows the expected format
pub fn assert_comment_name_format(
    plan: &PlanResult,
    expected_prefix: &str,
    expected_parent_name: &str,
) -> Result<(), String> {
    for change in &plan.changes {
        if let ChangeOperation::CreateObject { object, .. } = change {
            if object.object_type == ObjectType::Comment {
                let comment_name = &object.qualified_name.name;
                if comment_name.starts_with(expected_prefix) {
                    // Found a comment with the expected prefix
                    if comment_name.contains(expected_parent_name) {
                        return Ok(());
                    } else {
                        return Err(format!(
                            "Comment name '{}' doesn't contain expected parent name '{}'",
                            comment_name, expected_parent_name
                        ));
                    }
                }
            }
        }
    }
    
    Err(format!("No comment found with prefix '{}'", expected_prefix))
}

/// Check if comments are in the correct order relative to their parent objects
pub fn verify_comment_ordering(plan: &PlanResult) -> Result<(), String> {
    let mut object_positions = std::collections::HashMap::new();
    
    // First pass: record positions of non-comment objects
    for (i, change) in plan.changes.iter().enumerate() {
        match change {
            ChangeOperation::CreateObject { object, .. } |
            ChangeOperation::UpdateObject { object, .. } => {
                if object.object_type != ObjectType::Comment {
                    let key = format!("{}:{}", 
                        object_type_to_str(&object.object_type),
                        format_object_name(object)
                    );
                    object_positions.insert(key, i);
                }
            }
            _ => {}
        }
    }
    
    // Second pass: verify comments come after their parent objects
    for (i, change) in plan.changes.iter().enumerate() {
        if let ChangeOperation::CreateObject { object, .. } = change {
            if object.object_type == ObjectType::Comment {
                // Extract parent info from comment name
                if let Some((parent_type, parent_name)) = parse_comment_parent(&object.qualified_name.name) {
                    let parent_key = format!("{}:{}", parent_type, parent_name);
                    
                    if let Some(&parent_pos) = object_positions.get(&parent_key) {
                        if i <= parent_pos {
                            return Err(format!(
                                "Comment '{}' at position {} should come after parent at position {}",
                                object.qualified_name.name, i, parent_pos
                            ));
                        }
                    }
                }
            }
        }
    }
    
    Ok(())
}

fn object_type_to_str(obj_type: &ObjectType) -> &'static str {
    match obj_type {
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
    }
}

fn parse_comment_parent(comment_name: &str) -> Option<(&str, String)> {
    if let Some(colon_pos) = comment_name.find(':') {
        let object_type = &comment_name[..colon_pos];
        let object_name = &comment_name[colon_pos + 1..];
        
        // Remove trailing () for functions if present
        let clean_name = if object_name.ends_with("()") {
            &object_name[..object_name.len() - 2]
        } else {
            object_name
        };
        
        Some((object_type, clean_name.to_string()))
    } else {
        None
    }
}