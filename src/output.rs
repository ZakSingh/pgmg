/// Trait for handling output in a way that works for both CLI and library usage
pub trait OutputHandler: Send + Sync {
    /// Display a success message
    fn success(&self, message: &str);
    
    /// Display an error message
    fn error(&self, message: &str);
    
    /// Display an info message
    fn info(&self, message: &str);
    
    /// Display a warning message
    fn warning(&self, message: &str);
    
    /// Display a heading/section title
    fn heading(&self, message: &str);
    
    /// Display a status message (e.g., "Creating table...")
    fn status(&self, action: &str, message: &str);
    
    /// Display debug information (may be ignored in production)
    fn debug(&self, message: &str);
}

/// Library output handler that collects messages
pub struct LibraryOutputHandler {
    messages: std::sync::Mutex<Vec<(OutputLevel, String)>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputLevel {
    Success,
    Error,
    Info,
    Warning,
    Heading,
    Status,
    Debug,
}

impl LibraryOutputHandler {
    pub fn new() -> Self {
        Self {
            messages: std::sync::Mutex::new(Vec::new()),
        }
    }
    
    pub fn get_messages(&self) -> Vec<(OutputLevel, String)> {
        self.messages.lock().unwrap().clone()
    }
    
    fn add_message(&self, level: OutputLevel, message: &str) {
        self.messages.lock().unwrap().push((level, message.to_string()));
    }
}

impl OutputHandler for LibraryOutputHandler {
    fn success(&self, message: &str) {
        self.add_message(OutputLevel::Success, message);
    }
    
    fn error(&self, message: &str) {
        self.add_message(OutputLevel::Error, message);
    }
    
    fn info(&self, message: &str) {
        self.add_message(OutputLevel::Info, message);
    }
    
    fn warning(&self, message: &str) {
        self.add_message(OutputLevel::Warning, message);
    }
    
    fn heading(&self, message: &str) {
        self.add_message(OutputLevel::Heading, message);
    }
    
    fn status(&self, action: &str, message: &str) {
        self.add_message(OutputLevel::Status, &format!("{} {}", action, message));
    }
    
    fn debug(&self, message: &str) {
        self.add_message(OutputLevel::Debug, message);
    }
}

/// CLI output handler that prints to stdout with colors
#[cfg(feature = "cli")]
pub struct CliOutputHandler;

#[cfg(feature = "cli")]
impl OutputHandler for CliOutputHandler {
    fn success(&self, message: &str) {
        use owo_colors::OwoColorize;
        println!("{} {}", "✓".green(), message);
    }
    
    fn error(&self, message: &str) {
        use owo_colors::OwoColorize;
        eprintln!("{} {}", "✗".red(), message);
    }
    
    fn info(&self, message: &str) {
        println!("{}", message);
    }
    
    fn warning(&self, message: &str) {
        use owo_colors::OwoColorize;
        println!("{} {}", "⚠".yellow(), message);
    }
    
    fn heading(&self, message: &str) {
        use owo_colors::OwoColorize;
        println!("\n{}", message.bold());
    }
    
    fn status(&self, action: &str, message: &str) {
        use owo_colors::OwoColorize;
        println!("{:>12} {}", action.green().bold(), message);
    }
    
    fn debug(&self, message: &str) {
        use tracing::debug;
        debug!("{}", message);
    }
}

/// Silent output handler that discards all output
pub struct SilentOutputHandler;

impl OutputHandler for SilentOutputHandler {
    fn success(&self, _message: &str) {}
    fn error(&self, _message: &str) {}
    fn info(&self, _message: &str) {}
    fn warning(&self, _message: &str) {}
    fn heading(&self, _message: &str) {}
    fn status(&self, _action: &str, _message: &str) {}
    fn debug(&self, _message: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_output_handler() {
        let handler = LibraryOutputHandler::new();
        
        // Test various output levels
        handler.success("Success message");
        handler.error("Error message");
        handler.info("Info message");
        handler.warning("Warning message");
        handler.heading("Heading message");
        handler.status("Creating", "table users");
        handler.debug("Debug message");
        
        let messages = handler.get_messages();
        assert_eq!(messages.len(), 7);
        
        assert_eq!(messages[0], (OutputLevel::Success, "Success message".to_string()));
        assert_eq!(messages[1], (OutputLevel::Error, "Error message".to_string()));
        assert_eq!(messages[2], (OutputLevel::Info, "Info message".to_string()));
        assert_eq!(messages[3], (OutputLevel::Warning, "Warning message".to_string()));
        assert_eq!(messages[4], (OutputLevel::Heading, "Heading message".to_string()));
        assert_eq!(messages[5], (OutputLevel::Status, "Creating table users".to_string()));
        assert_eq!(messages[6], (OutputLevel::Debug, "Debug message".to_string()));
    }

    #[test]
    fn test_silent_output_handler() {
        let handler = SilentOutputHandler;
        
        // These should all be no-ops
        handler.success("Success message");
        handler.error("Error message");
        handler.info("Info message");
        handler.warning("Warning message");
        handler.heading("Heading message");
        handler.status("Creating", "table users");
        handler.debug("Debug message");
        
        // No way to verify silent output, but we ensure it doesn't panic
    }

    #[test]
    fn test_output_level_equality() {
        assert_eq!(OutputLevel::Success, OutputLevel::Success);
        assert_ne!(OutputLevel::Success, OutputLevel::Error);
        assert_ne!(OutputLevel::Info, OutputLevel::Warning);
    }

    #[test]
    fn test_library_handler_thread_safety() {
        use std::sync::Arc;
        use std::thread;
        
        let handler = Arc::new(LibraryOutputHandler::new());
        let mut handles = vec![];
        
        for i in 0..10 {
            let handler_clone = Arc::clone(&handler);
            let handle = thread::spawn(move || {
                handler_clone.info(&format!("Message from thread {}", i));
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let messages = handler.get_messages();
        assert_eq!(messages.len(), 10);
        
        // All messages should be info level
        for (level, _) in messages {
            assert_eq!(level, OutputLevel::Info);
        }
    }

    #[cfg(feature = "cli")]
    #[test]
    fn test_cli_output_handler() {
        // We can't easily test console output, but we can ensure it doesn't panic
        let handler = CliOutputHandler;
        
        handler.success("Success message");
        handler.error("Error message");
        handler.info("Info message");
        handler.warning("Warning message");
        handler.heading("Heading message");
        handler.status("Creating", "table users");
        handler.debug("Debug message");
    }
}