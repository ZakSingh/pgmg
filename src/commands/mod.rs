pub mod plan;
pub mod apply;
pub mod watch;
pub mod reset;
pub mod test;
pub mod seed;
pub mod new;
pub mod check;
pub mod run;

pub use plan::{execute_plan, PlanResult, ChangeOperation};
pub use apply::{execute_apply, ApplyResult};
pub use watch::{execute_watch, WatchConfig};
pub use reset::{execute_reset, ResetResult};
pub use test::{execute_test, execute_test_with_options, TestResult};
pub use seed::{execute_seed, SeedResult};
pub use new::{execute_new, NewResult};
pub use check::{execute_check, CheckResult};
pub use run::{execute_run, run_sql_file};

#[cfg(feature = "cli")]
pub use plan::print_plan_summary;
#[cfg(feature = "cli")]
pub use apply::print_apply_summary;
#[cfg(feature = "cli")]
pub use reset::print_reset_summary;
#[cfg(feature = "cli")]
pub use test::print_test_summary;
#[cfg(feature = "cli")]
pub use seed::print_seed_summary;
#[cfg(feature = "cli")]
pub use new::print_new_summary;
#[cfg(feature = "cli")]
pub use check::print_check_summary;