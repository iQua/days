//! Repository audit and evidence tooling for the Days Executor program.
//!
//! The normative contract is documented in
//! `docs/days-executor/audit-contract.md` at the workspace root.

pub mod audit;
pub mod baseline;
pub mod dependency_baseline;
pub mod diagnostics;
pub mod hash;
pub mod reproduce;
pub mod schema;

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Failures that prevent an xtask operation from being started.
#[derive(Debug, Error)]
pub enum XtaskError {
    /// No workspace root could be found above the starting directory.
    #[error("could not find a workspace Cargo.toml from {0}")]
    WorkspaceRootNotFound(PathBuf),
    /// The current directory could not be read.
    #[error("could not read the current directory: {0}")]
    CurrentDirectory(#[source] std::io::Error),
}

/// Finds the workspace root by walking upward from `start`.
pub fn discover_repo_root(start: &Path) -> Result<PathBuf, XtaskError> {
    let mut candidate = start.to_path_buf();
    loop {
        let manifest = candidate.join("Cargo.toml");
        if manifest.is_file() {
            if let Ok(contents) = std::fs::read_to_string(&manifest) {
                if contents.contains("[workspace]") {
                    return Ok(candidate);
                }
            }
        }
        if !candidate.pop() {
            return Err(XtaskError::WorkspaceRootNotFound(start.to_path_buf()));
        }
    }
}
