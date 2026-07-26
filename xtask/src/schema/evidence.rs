//! Evidence manifest schema.

use serde::{Deserialize, Serialize};

use super::{
    SchemaError, SchemaVersion, invalid, is_phase_id, is_task_id, validate_git_commit,
    validate_hash, validate_nonempty, validate_repo_path,
};

/// Whether evidence is checked into this repository or stored in `days-gpu`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EvidenceKind {
    /// Small evidence stored and checksummed in the repository.
    Golden,
    /// Large evidence committed to the companion `days-gpu` repository.
    Archive,
    /// Measurement evidence bound to a frozen budget and run commit.
    Measurement,
}

/// Versioned evidence for one phase task.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceManifest {
    /// Schema version. Only version 1 is accepted.
    pub schema_version: SchemaVersion,
    /// Stable evidence-manifest identifier.
    pub id: String,
    /// Owning phase identifier.
    pub phase: String,
    /// Owning task identifier.
    pub task: String,
    /// Storage policy applied to all artifacts.
    pub kind: EvidenceKind,
    /// Human-readable evidence description.
    pub description: String,
    /// Reproduction command as an argv array.
    pub command: Vec<String>,
    /// Version of the tool used to produce the evidence.
    pub tool_version: String,
    /// Versioned evidence-data schema identifier.
    pub schema: String,
    /// Machine-readable evidence capability tags.
    pub tags: Vec<String>,
    /// Budget manifest used by a measurement, if applicable.
    #[serde(default)]
    pub budget: Option<String>,
    /// Hash of `budget` at measurement time, if applicable.
    #[serde(default)]
    pub budget_hash: Option<String>,
    /// Commit containing the code used for a measurement run, if applicable.
    #[serde(default)]
    pub run_commit: Option<String>,
    /// Evidence artifacts governed by this manifest.
    pub artifacts: Vec<EvidenceArtifact>,
}

impl EvidenceManifest {
    pub(super) fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("id", &self.id)?;
        if !is_phase_id(&self.phase) {
            return Err(invalid("phase", "must match `P[0-9]{2}`"));
        }
        if !is_task_id(&self.task) {
            return Err(invalid("task", "must match `T[0-9]+[A-Z]*`"));
        }
        validate_nonempty("description", &self.description)?;
        if self.artifacts.is_empty() {
            return Err(invalid("artifacts", "must contain at least one artifact"));
        }

        if self.kind != EvidenceKind::Measurement
            && self.budget_hash.is_some()
            && self.budget.is_none()
        {
            return Err(SchemaError::BudgetHashWithoutBudget);
        }
        if let Some(budget) = &self.budget {
            validate_repo_path("budget", budget)?;
        }
        if let Some(hash) = &self.budget_hash {
            validate_hash("budget_hash", hash)?;
        }
        if let Some(commit) = &self.run_commit {
            validate_git_commit("run_commit", commit)?;
        }
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        Ok(())
    }
}

/// One artifact in an evidence manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceArtifact {
    /// Repository-relative path in this repository or in `days-gpu`.
    #[serde(default)]
    pub path: Option<String>,
    /// Commit in `days-gpu` containing an archived artifact.
    #[serde(default)]
    pub days_gpu_commit: Option<String>,
    /// SHA-256 hash of the artifact bytes.
    #[serde(default)]
    pub content_hash: Option<String>,
    /// Version of the tool used to produce an archived artifact.
    #[serde(default)]
    pub tool_version: Option<String>,
    /// Exact argv used to produce an archived artifact.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Versioned schema identifier for an archived artifact.
    #[serde(default)]
    pub schema: Option<String>,
}

impl EvidenceArtifact {
    fn validate(&self) -> Result<(), SchemaError> {
        if let Some(path) = &self.path {
            validate_repo_path("artifacts.path", path)?;
        }
        if let Some(commit) = &self.days_gpu_commit {
            validate_nonempty("artifacts.days_gpu_commit", commit)?;
        }
        if let Some(hash) = &self.content_hash {
            validate_hash("artifacts.content_hash", hash)?;
        }
        if let Some(tool_version) = &self.tool_version {
            validate_nonempty("artifacts.tool_version", tool_version)?;
        }
        if let Some(command) = &self.command {
            if command.is_empty() || command.iter().any(String::is_empty) {
                return Err(invalid(
                    "artifacts.command",
                    "must contain a non-empty executable and non-empty arguments",
                ));
            }
        }
        if let Some(schema) = &self.schema {
            validate_nonempty("artifacts.schema", schema)?;
        }
        Ok(())
    }
}
