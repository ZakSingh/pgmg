use std::io::IsTerminal;
use tracing::Level;
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::UtcTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// Initialize the logging and error reporting infrastructure
pub fn init(verbosity: u8) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Install color-eyre panic and error handlers if available
    #[cfg(feature = "cli")]
    color_eyre::install()?;
    
    // Set up the logging level based on verbosity
    let log_level = match verbosity {
        0 => Level::WARN,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };
    
    // Create the env filter, allowing RUST_LOG to override
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("pgmg={},tokio_postgres=warn,hyper=warn", log_level)));
    
    // Check if we're running in a terminal for color output
    let is_terminal = std::io::stdout().is_terminal();
    
    // Set up the formatting layer
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(is_terminal)
        .with_timer(UtcTime::rfc_3339())
        .with_span_events(FmtSpan::CLOSE);
    
    // Combine layers and set as global subscriber
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
    
    Ok(())
}

/// Progress indicator for long-running operations
pub struct Progress {
    message: String,
    count: usize,
    total: Option<usize>,
}

impl Progress {
    pub fn new(message: impl Into<String>) -> Self {
        let message = message.into();
        println!("{}", message);
        Self {
            message,
            count: 0,
            total: None,
        }
    }
    
    pub fn with_total(message: impl Into<String>, total: usize) -> Self {
        let message = message.into();
        println!("{} (0/{})", message, total);
        Self {
            message,
            count: 0,
            total: Some(total),
        }
    }
    
    pub fn increment(&mut self) {
        self.count += 1;
        self.update();
    }
    
    pub fn update(&self) {
        if let Some(total) = self.total {
            // Clear the line and rewrite
            print!("\r{} ({}/{})", self.message, self.count, total);
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }
    
    pub fn finish(&self) {
        if self.total.is_some() {
            println!(); // New line after progress
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Logging macros that include structured context

#[macro_export]
macro_rules! log_error {
    ($err:expr) => {
        tracing::error!(
            error = %$err,
            "Operation failed"
        );
        if let Some(suggestion) = $crate::error::suggest_fix(&$err) {
            tracing::info!("{}", suggestion);
        }
    };
    ($err:expr, $($key:tt = $value:expr),+ $(,)?) => {
        tracing::error!(
            error = %$err,
            $($key = $value,)+
            "Operation failed"
        );
        if let Some(suggestion) = $crate::error::suggest_fix(&$err) {
            tracing::info!("{}", suggestion);
        }
    };
}

#[macro_export]
macro_rules! log_sql_error {
    ($err:expr, $file:expr, $statement:expr) => {
        tracing::error!(
            error = %$err,
            file = %$file.display(),
            statement = $statement,
            "SQL execution failed"
        );
    };
}

#[macro_export]
macro_rules! log_migration {
    ($name:expr, $action:expr) => {
        tracing::info!(
            migration = $name,
            action = $action,
            "Migration event"
        );
    };
}

#[macro_export]
macro_rules! log_object_change {
    ($object_type:expr, $object_name:expr, $action:expr) => {
        tracing::info!(
            object_type = $object_type,
            object_name = $object_name,
            action = $action,
            "Object change"
        );
    };
}

/// Format output for CLI with colors
pub mod output {
    #[cfg(feature = "cli")]
    use console::{style, Emoji};
    use std::fmt::Display;
    
    #[cfg(feature = "cli")]
    static CHECKMARK: Emoji<'_, '_> = Emoji("✓ ", "[OK] ");
    #[cfg(feature = "cli")]
    static CROSS: Emoji<'_, '_> = Emoji("✗ ", "[FAIL] ");
    #[cfg(feature = "cli")]
    static ARROW: Emoji<'_, '_> = Emoji("→ ", "-> ");
    #[cfg(feature = "cli")]
    static WARNING: Emoji<'_, '_> = Emoji("⚠ ", "[WARN] ");
    #[cfg(feature = "cli")]
    static INFO: Emoji<'_, '_> = Emoji("ℹ ", "[INFO] ");
    
    pub fn success(message: impl Display) {
        println!("{} {}", style(CHECKMARK).green(), message);
    }
    
    pub fn error(message: impl Display) {
        eprintln!("{} {}", style(CROSS).red(), style(message).red());
    }
    
    pub fn warning(message: impl Display) {
        println!("{} {}", style(WARNING).yellow(), style(message).yellow());
    }
    
    pub fn info(message: impl Display) {
        println!("{} {}", style(INFO).blue(), message);
    }
    
    pub fn step(message: impl Display) {
        println!("{} {}", style(ARROW).cyan(), message);
    }
    
    pub fn header(message: impl Display) {
        println!("\n{}", style(message).bold().underlined());
    }
    
    pub fn subheader(message: impl Display) {
        println!("\n{}", style(message).bold());
    }
}

/// Helper to format durations in human-readable format
pub fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    
    if secs == 0 {
        format!("{}ms", millis)
    } else if secs < 60 {
        format!("{}.{:03}s", secs, millis)
    } else {
        let mins = secs / 60;
        let secs = secs % 60;
        format!("{}m {}s", mins, secs)
    }
}