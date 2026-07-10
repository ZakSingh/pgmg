pub mod parser;
pub mod splitter;
pub mod objects;
pub mod test_analyzer;
pub mod migration_analyzer;

pub use parser::{
    analyze_statement, analyze_plpgsql, filter_builtins,
    Dependencies, QualifiedIdent
};
pub use splitter::{split_sql_file, SqlStatement};
pub use objects::{
    identify_sql_object, calculate_ddl_hash, SqlObject, ObjectType,
    format_qualified_name, state_object_name, parse_state_object_name, human_object_name,
};
pub use test_analyzer::{analyze_test_file, scan_test_files, build_test_dependency_map, TestFile, TestDependencyMap};
pub use migration_analyzer::{extract_altered_tables, extract_enum_add_value_statements};