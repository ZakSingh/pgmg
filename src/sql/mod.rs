pub mod parser;
pub mod splitter;
pub mod objects;
pub mod test_analyzer;
pub mod migration_analyzer;
pub mod late_binding;

pub use parser::{
    analyze_statement, analyze_plpgsql, filter_builtins,
    Dependencies, QualifiedIdent
};
pub use splitter::{split_sql_file, SqlStatement};
pub use objects::{
    identify_sql_object, calculate_ddl_hash, SqlObject, ObjectType,
    format_qualified_name, state_object_name, parse_state_object_name, human_object_name,
    with_or_replace,
};
pub use test_analyzer::{analyze_test_file, scan_test_files, build_test_dependency_map, TestFile, TestDependencyMap};
pub use migration_analyzer::{
    analyze_table_alterations, analyze_type_changes, definition_sensitive_to_added_columns,
    extract_enum_add_value_statements, AlterImpact, MigrationAnalysis, TableAlteration,
};
pub use late_binding::{
    has_late_bound_body, has_sql_standard_body, is_sql_language_routine, references_only_from_body,
    routine_header_identifiers,
};
