use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const TRACE_MANIFEST_FILENAME: &str = "traces.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceManifestV1 {
    pub version: u32,
    pub traces: Vec<String>,
}

impl TraceManifestV1 {
    pub fn new(traces: Vec<String>) -> Self {
        Self { version: 1, traces }
    }
}

pub fn manifest_path(log_path: &Path) -> PathBuf {
    log_path.join(TRACE_MANIFEST_FILENAME)
}

pub fn write_manifest_v1(log_path: &Path, traces: Vec<String>) -> Result<(), String> {
    let manifest = TraceManifestV1::new(traces);
    let content = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| format!("Failed to serialize trace manifest JSON: {e}"))?;

    let path = manifest_path(log_path);
    fs::write(&path, content).map_err(|e| format!("Failed to write {}: {e}", path.display()))?;
    Ok(())
}
