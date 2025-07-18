use crate::commands::{execute_plan, execute_apply};
use crate::error::{PgmgError, Result};
use crate::logging::output;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

/// Configuration for the watch command
#[derive(Debug)]
pub struct WatchConfig {
    pub migrations_dir: Option<PathBuf>,
    pub code_dir: Option<PathBuf>,
    pub connection_string: String,
    pub debounce_duration: Duration,
    pub auto_apply: bool,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            migrations_dir: None,
            code_dir: None,
            connection_string: String::new(),
            debounce_duration: Duration::from_millis(500),
            auto_apply: true,
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
                        process_changes(&config, paths).await;
                    }
                }
            }
        }
    }
}

/// Process a set of file changes
async fn process_changes(config: &WatchConfig, paths: HashSet<PathBuf>) {
    output::step(&format!("Detected changes in {} file(s)", paths.len()));
    
    for path in &paths {
        info!("  - {}", path.display());
    }
    
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
                return;
            }
            
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
                        }
                        crate::commands::plan::ChangeOperation::UpdateObject { object, .. } => {
                            println!("  ~ {:?} {}", object.object_type, object.qualified_name.name);
                        }
                        crate::commands::plan::ChangeOperation::DeleteObject { object_type, object_name, .. } => {
                            println!("  - {:?} {}", object_type, object_name);
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
                            for error in &apply_result.errors {
                                error!("  Apply failed: {}", error);
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
        }
        Err(e) => {
            output::error(&format!("Failed to plan changes: {}", e));
            let pgmg_error = PgmgError::from(e);
            if let Some(suggestion) = crate::error::suggest_fix(&pgmg_error) {
                output::info(&suggestion);
            }
        }
    }
    
    output::info("Watching for changes...");
}