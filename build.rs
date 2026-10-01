//! Embeds the git commit the binary was built from so `pgmg --version` can
//! report it. CI sets `PGMG_GIT_SHA` explicitly; local builds fall back to
//! asking git, and builds without either report "unknown".

use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=PGMG_GIT_SHA");

    let sha = std::env::var("PGMG_GIT_SHA")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(sha_from_git)
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=PGMG_GIT_SHA={sha}");
}

fn sha_from_git() -> Option<String> {
    // Rebuild when HEAD moves (new commit, checkout) so a stale sha is not
    // baked into a fresh binary. Paths come from git so worktrees, where
    // `.git` is a file pointing elsewhere, are handled too.
    if let Some(head_path) = git(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
        if let Ok(head) = std::fs::read_to_string(&head_path) {
            if let Some(reference) = head.trim().strip_prefix("ref: ") {
                if let Some(ref_path) = git(&["rev-parse", "--git-path", reference]) {
                    if Path::new(&ref_path).exists() {
                        println!("cargo:rerun-if-changed={ref_path}");
                    }
                }
            }
        }
    }

    let sha = git(&["rev-parse", "HEAD"])?;
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
        .map(|s| !s.is_empty())
        .unwrap_or(false);

    Some(if dirty { format!("{sha}-dirty") } else { sha })
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    Some(text.to_string())
}
