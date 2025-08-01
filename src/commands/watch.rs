use crate::commands::{execute_plan, execute_apply, execute_test_with_options};
use crate::config::PgmgConfig;
use crate::error::{PgmgError, Result};
use crate::logging::output;
use crate::sql::{scan_test_files, build_test_dependency_map, TestDependencyMap};
use crate::analysis::graph::ObjectRef;
use crate::builtin_catalog::BuiltinCatalog;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use owo_colors::OwoColorize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Configuration for the watch command
#[derive(Debug)]
pub struct WatchConfig {
    pub migrations_dir: Option<PathBuf>,
    pub code_dir: Option<PathBuf>,
    pub connection_string: String,
    pub debounce_duration: Duration,
    pub auto_apply: bool,
    pub pgmg_config: PgmgConfig,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            migrations_dir: None,
            code_dir: None,
            connection_string: String::new(),
            debounce_duration: Duration::from_millis(500),
            auto_apply: true,
            pgmg_config: PgmgConfig::default(),
        }
    }
}

/// State for tracking file changes and debouncing
#[derive(Debug)]
struct WatchState {
    last_event_time: Instant,
    pending_paths: HashSet<PathBuf>,
}

impl WatchState {
    fn new() -> Self {
        Self {
            last_event_time: Instant::now(),
            pending_paths: HashSet::new(),
        }
    }

    fn add_path(&mut self, path: PathBuf) {
        self.pending_paths.insert(path);
        self.last_event_time = Instant::now();
    }

    fn should_process(&self, debounce_duration: Duration) -> bool {
        !self.pending_paths.is_empty() && 
        self.last_event_time.elapsed() >= debounce_duration
    }

    fn take_paths(&mut self) -> HashSet<PathBuf> {
        std::mem::take(&mut self.pending_paths)
    }
}

/// Execute the watch command
pub async fn execute_watch(config: WatchConfig) -> Result<()> {
    output::header("Watch Mode");
    info!("Starting file watcher...");
    
    // Validate directories exist
    if let Some(ref dir) = config.migrations_dir {
        if !dir.exists() {
            return Err(PgmgError::DirectoryNotFound(dir.clone()));
        }
        info!("Watching migrations directory: {}", dir.display());
    }
    
    if let Some(ref dir) = config.code_dir {
        if !dir.exists() {
            return Err(PgmgError::DirectoryNotFound(dir.clone()));
        }
        info!("Watching code directory: {}", dir.display());
    }
    
    if config.migrations_dir.is_none() && config.code_dir.is_none() {
        return Err(PgmgError::Configuration(
            "No directories specified to watch. Use --migrations-dir or --code-dir".to_string()
        ));
    }
    
    // Create a channel for file events
    let (tx, rx) = mpsc::channel();
    
    // Create a watcher
    let mut watcher = RecommendedWatcher::new(
        move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                // Only care about modifications and creations
                match event.kind {
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_) => {
                        for path in event.paths {
                            // Only watch SQL files
                            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                                let _ = tx.send(path);
                            }
                        }
                    }
                    _ => {}
                }
            }
        },
        Config::default(),
    ).map_err(|e| PgmgError::WatchError {
        path: PathBuf::from("."),
        message: format!("Failed to create file watcher: {}", e),
    })?;
    
    // Add paths to watcher
    if let Some(ref dir) = config.migrations_dir {
        watcher.watch(dir, RecursiveMode::Recursive)
            .map_err(|e| PgmgError::WatchError {
                path: dir.clone(),
                message: format!("Failed to watch directory: {}", e),
            })?;
    }
    
    if let Some(ref dir) = config.code_dir {
        watcher.watch(dir, RecursiveMode::Recursive)
            .map_err(|e| PgmgError::WatchError {
                path: dir.clone(),
                message: format!("Failed to watch directory: {}", e),
            })?;
    }
    
    output::success("File watcher started. Press Ctrl+C to stop.");
    output::info(&format!(
        "Watching for changes (debounce: {}ms, auto-apply: {})",
        config.debounce_duration.as_millis(),
        if config.auto_apply { "enabled" } else { "disabled" }
    ));
    
    // Build initial test dependency map
    let test_dep_map = Arc::new(Mutex::new(None::<TestDependencyMap>));
    if let Some(ref code_dir) = config.code_dir {
        output::step("Analyzing test dependencies...");
        match build_test_dependencies(code_dir).await {
            Ok(dep_map) => {
                let test_count = dep_map.tests.len();
                match test_dep_map.lock() {
                    Ok(mut guard) => {
                        *guard = Some(dep_map);
                        output::info(&format!("Found {} test files with dependencies", test_count));
                    }
                    Err(e) => {
                        output::error(&format!("Mutex poisoned while updating test dependencies: {}", e));
                    }
                }
            }
            Err(e) => {
                output::error(&format!("Failed to analyze test dependencies: {}", e));
            }
        }
    }
    
    // Create shared state for debouncing
    let mut state = WatchState::new();
    
    // Handle incoming file events and process them
    loop {
        // Check for new events with a timeout
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(path) => {
                debug!("File changed: {}", path.display());
                state.add_path(path);
            }
            Err(_) => {
                // Timeout or channel closed
                // Check if we should process pending changes
                if state.should_process(config.debounce_duration) {
                    let paths = state.take_paths();
                    if !paths.is_empty() {
                        process_changes(&config, paths, test_dep_map.clone()).await;
                    }
                }
            }
        }
    }
}

/// Process a set of file changes
async fn process_changes(
    config: &WatchConfig, 
    paths: HashSet<PathBuf>,
    test_dep_map: Arc<Mutex<Option<TestDependencyMap>>>,
) {
    output::step(&format!("Detected changes in {} file(s)", paths.len()));
    
    // Separate test files from database object files
    let mut test_files = Vec::new();
    let mut db_files = Vec::new();
    
    for path in &paths {
        info!("  - {}", path.display());
        
        if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
            if file_name.contains(".test.") {
                test_files.push(path.clone());
            } else {
                db_files.push(path.clone());
            }
        }
    }
    
    // Process database object changes first (if any)
    let mut changed_objects = Vec::new();
    if !db_files.is_empty() {
        output::step("Processing database object changes...");
        changed_objects = process_db_changes(config, db_files).await;
    }
    
    // Rebuild test dependency map if any test files changed
    if !test_files.is_empty() && config.code_dir.is_some() {
        output::step("Rebuilding test dependency map...");
        if let Some(ref code_dir) = config.code_dir {
            if let Ok(new_map) = build_test_dependencies(code_dir).await {
                match test_dep_map.lock() {
                    Ok(mut guard) => {
                        *guard = Some(new_map);
                    }
                    Err(e) => {
                        output::error(&format!("Mutex poisoned while rebuilding test dependencies: {}", e));
                    }
                }
            }
        }
    }
    
    // Run tests for changed test files
    if !test_files.is_empty() {
        output::step("Running tests for changed test files...");
        run_specific_tests(config, test_files).await;
    }
    
    // Run tests affected by changed database objects
    if !changed_objects.is_empty() {
        match test_dep_map.lock() {
            Ok(guard) => {
                if let Some(ref dep_map) = *guard {
                    let affected_tests = dep_map.find_tests_for_objects(&changed_objects);
                    if !affected_tests.is_empty() {
                        output::step(&format!("Running {} tests affected by database changes...", affected_tests.len()));
                        run_specific_tests(config, affected_tests).await;
                    }
                }
            }
            Err(e) => {
                output::error(&format!("Mutex poisoned while checking test dependencies: {}", e));
            }
        }
    }
}

/// Process database object file changes (plan and apply)
async fn process_db_changes(config: &WatchConfig, _paths: Vec<PathBuf>) -> Vec<ObjectRef> {
    // Run plan
    output::step("Running plan...");
    
    match execute_plan(
        config.migrations_dir.clone(),
        config.code_dir.clone(),
        config.connection_string.clone(),
        None, // No graph output in watch mode
    ).await {
        Ok(plan_result) => {
            // Check if there are any changes
            if plan_result.changes.is_empty() && plan_result.new_migrations.is_empty() {
                output::info("No changes detected");
                return Vec::new();
            }
            
            // Collect changed objects for test dependency analysis
            let mut changed_objects = Vec::new();
            
            // Show plan summary
            output::subheader("Changes detected:");
            
            if !plan_result.new_migrations.is_empty() {
                println!("New migrations:");
                for migration in &plan_result.new_migrations {
                    println!("  + {}", migration);
                }
            }
            
            if !plan_result.changes.is_empty() {
                println!("Object changes:");
                for change in &plan_result.changes {
                    match change {
                        crate::commands::plan::ChangeOperation::CreateObject { object, .. } => {
                            println!("  + {:?} {}", object.object_type, object.qualified_name.name);
                            changed_objects.push(ObjectRef {
                                object_type: object.object_type.clone(),
                                qualified_name: object.qualified_name.clone(),
                            });
                        }
                        crate::commands::plan::ChangeOperation::UpdateObject { object, .. } => {
                            println!("  ~ {:?} {}", object.object_type, object.qualified_name.name);
                            changed_objects.push(ObjectRef {
                                object_type: object.object_type.clone(),
                                qualified_name: object.qualified_name.clone(),
                            });
                        }
                        crate::commands::plan::ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                            println!("  - {:?} {}", object_type, object_name);
                            // Deleted objects don't need test runs
                        }
                        crate::commands::plan::ChangeOperation::ApplyMigration { name, .. } => {
                            println!("  > Migration {}", name);
                        }
                    }
                }
            }
            
            // Auto-apply if enabled
            if config.auto_apply {
                output::step("Applying changes...");
                
                match execute_apply(
                    config.migrations_dir.clone(),
                    config.code_dir.clone(),
                    config.connection_string.clone(),
                    &config.pgmg_config,
                ).await {
                    Ok(apply_result) => {
                        if apply_result.errors.is_empty() {
                            output::success(&format!(
                                "Successfully applied {} changes",
                                apply_result.migrations_applied.len() + 
                                apply_result.objects_created.len() + 
                                apply_result.objects_updated.len() +
                                apply_result.objects_deleted.len()
                            ));
                        } else {
                            output::error(&format!(
                                "Apply completed with {} error(s)",
                                apply_result.errors.len()
                            ));
                            // Display each error with proper formatting preserved
                            for error in &apply_result.errors {
                                // The error already includes detailed formatting from apply command
                                println!("\n{}", error);
                            }
                        }
                    }
                    Err(e) => {
                        output::error(&format!("Failed to apply changes: {}", e));
                        let pgmg_error = PgmgError::from(e);
                        if let Some(suggestion) = crate::error::suggest_fix(&pgmg_error) {
                            output::info(&suggestion);
                        }
                    }
                }
            } else {
                output::info("Auto-apply is disabled. Run 'pgmg apply' to apply changes.");
            }
            
            // Return changed objects
            changed_objects
        }
        Err(e) => {
            output::error(&format!("Failed to plan changes: {}", e));
            let pgmg_error = PgmgError::from(e);
            if let Some(suggestion) = crate::error::suggest_fix(&pgmg_error) {
                output::info(&suggestion);
            }
            Vec::new()
        }
    }
}

/// Build test dependency map for the code directory
async fn build_test_dependencies(code_dir: &Path) -> std::result::Result<TestDependencyMap, Box<dyn std::error::Error>> {
    let builtin_catalog = BuiltinCatalog::new();
    let test_files = scan_test_files(code_dir, &builtin_catalog).await?;
    Ok(build_test_dependency_map(test_files))
}

/// Run specific test files
async fn run_specific_tests(config: &WatchConfig, test_files: Vec<PathBuf>) {
    for test_file in test_files {
        info!("Running test: {}", test_file.display());
        
        match execute_test_with_options(
            Some(test_file.clone()),
            config.connection_string.clone(),
            false, // Don't show TAP output in watch mode
            false, // Don't show immediate results (we'll show our own)
            true,  // Run quietly in watch mode
            &config.pgmg_config,
        ).await {
            Ok(test_result) => {
                // Display relative path from current directory
                let display_path = std::env::current_dir()
                    .ok()
                    .and_then(|cwd| test_file.strip_prefix(cwd).ok())
                    .unwrap_or(&test_file);
                    
                if test_result.tests_failed == 0 {
                    output::success(&format!(
                        "✓ {} - {} tests passed",
                        display_path.display(),
                        test_result.tests_passed
                    ));
                } else {
                    output::error(&format!(
                        "❌ {} - {} failed, {} passed",
                        display_path.display(),
                        test_result.tests_failed,
                        test_result.tests_passed
                    ));
                    
                    // Show failures with enhanced formatting
                    for file_result in &test_result.test_files {
                        for failure in &file_result.failures {
                            println!("    {} {}: {}", "✗".red(), failure.test_number, failure.description);
                            
                            // Show detailed error if available (SQL execution errors)
                            if let Some(detailed_error) = &failure.detailed_error {
                                // The detailed error already includes formatting, so just print it with indentation
                                for line in detailed_error.lines() {
                                    println!("      {}", line);
                                }
                            } else if let Some(diagnostic) = &failure.diagnostic {
                                // Show pgtap diagnostic information with proper formatting
                                println!("      {}: {}", "Diagnostic".yellow().bold(), "");
                                for diag_line in diagnostic.lines() {
                                    if diag_line.trim().is_empty() {
                                        continue;
                                    }
                                    
                                    // Format specific pgtap diagnostic patterns
                                    if diag_line.contains("Failed test") {
                                        println!("        {}: {}", "Test".dimmed(), diag_line.replace("Failed test", "").trim().trim_matches('"').yellow());
                                    } else if diag_line.contains("got:") || diag_line.contains("Got:") {
                                        let got_value = diag_line.split(':').nth(1).unwrap_or("").trim();
                                        println!("        {}: {}", "Got".red().bold(), got_value.red());
                                    } else if diag_line.contains("expected:") || diag_line.contains("Expected:") {
                                        let expected_value = diag_line.split(':').nth(1).unwrap_or("").trim();
                                        println!("        {}: {}", "Expected".green().bold(), expected_value.green());
                                    } else if diag_line.contains("DETAIL:") {
                                        let detail = diag_line.replace("DETAIL:", "").trim().to_string();
                                        println!("        {}: {}", "Detail".yellow(), detail);
                                    } else if diag_line.contains("HINT:") {
                                        let hint = diag_line.replace("HINT:", "").trim().to_string();
                                        println!("        {}: {}", "Hint".green(), hint);
                                    } else if diag_line.contains("caught:") {
                                        let caught_value = diag_line.split(':').skip(1).collect::<Vec<_>>().join(":").trim().to_string();
                                        println!("        {}: {}", "Caught".red().bold(), caught_value.red());
                                    } else if diag_line.contains("wanted:") {
                                        let wanted_value = diag_line.split(':').skip(1).collect::<Vec<_>>().join(":").trim().to_string();
                                        println!("        {}: {}", "Expected".green().bold(), wanted_value.green());
                                    } else {
                                        // Generic diagnostic line
                                        println!("        {}", diag_line.bright_black());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                output::error(&format!("Failed to run test {}: {}", test_file.display(), e));
            }
        }
    }
}