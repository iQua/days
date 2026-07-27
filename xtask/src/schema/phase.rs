//! Phase metadata schema.

use serde::{Deserialize, Serialize};

use super::{
    SchemaError, SchemaVersion, invalid, is_phase_id, is_task_dependency, is_task_id,
    validate_hash, validate_nonempty, validate_repo_path,
};

/// Machine-readable metadata for one program phase.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseMetadata {
    /// Schema version. Only version 1 is accepted.
    pub schema_version: SchemaVersion,
    /// Stable phase identifier such as `P01`.
    pub phase: String,
    /// File-stem name for the phase.
    pub name: String,
    /// Human-readable phase title.
    pub title: String,
    /// Repository-relative path to the phase design note.
    pub design_note: String,
    /// Phase identifiers that this phase depends on.
    pub depends_on: Vec<String>,
    /// External dependency identifiers not represented by phase metadata.
    pub external_depends_on: Vec<String>,
    /// Cargo features introduced or exercised by the phase.
    pub features: Vec<String>,
    /// Platform identifiers covered by the phase test matrix.
    pub platforms: Vec<String>,
    /// Descriptions of simulator semantic changes.
    pub semantic_changes: Vec<String>,
    /// Descriptions of public API or configuration changes.
    pub api_changes: Vec<String>,
    /// Whether the phase changes the trusted computing boundary.
    pub trust_boundary_change: bool,
    /// Whether the phase changes formal proofs.
    pub proof_changing: bool,
    /// Work explicitly excluded from this phase.
    pub out_of_scope: Vec<String>,
    /// Backends selectable at runtime after this phase.
    pub selectable_backends: Vec<String>,
    /// Tasks delivered by this phase.
    pub tasks: Vec<PhaseTask>,
    /// Deterministic commands that verify the phase.
    pub test_commands: Vec<TestCommand>,
    /// Backend status declarations.
    #[serde(default)]
    pub backends: Vec<Backend>,
    /// Performance-budget references.
    #[serde(default)]
    pub budgets: Vec<BudgetReference>,
}

impl PhaseMetadata {
    pub(super) fn validate(&self) -> Result<(), SchemaError> {
        if !is_phase_id(&self.phase) {
            return Err(invalid("phase", "must match `P[0-9]{2}`"));
        }
        validate_nonempty("name", &self.name)?;
        validate_nonempty("title", &self.title)?;
        validate_repo_path("design_note", &self.design_note)?;
        if self.tasks.is_empty() {
            return Err(invalid("tasks", "must contain at least one task"));
        }

        for dependency in &self.depends_on {
            if !is_phase_id(dependency) {
                return Err(invalid("depends_on", "entries must match `P[0-9]{2}`"));
            }
        }

        for task in &self.tasks {
            task.validate()?;
        }
        for command in &self.test_commands {
            command.validate()?;
        }
        for (index, budget) in self.budgets.iter().enumerate() {
            budget.validate()?;
            if self.budgets[..index]
                .iter()
                .any(|prior| prior.id == budget.id)
            {
                return Err(invalid(
                    "budgets.id",
                    "must be unique within phase metadata",
                ));
            }
        }

        Ok(())
    }
}

/// Metadata for one task within a phase.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseTask {
    /// Task identifier such as `T0`.
    pub id: String,
    /// Human-readable task title.
    pub title: String,
    /// Same-phase or fully qualified task dependencies.
    pub depends_on: Vec<String>,
    /// Repository-relative evidence-manifest paths.
    pub evidence: Vec<String>,
    /// Red-test declaration, when present.
    #[serde(default)]
    pub red_test: Option<RedTest>,
}

impl PhaseTask {
    fn validate(&self) -> Result<(), SchemaError> {
        if !is_task_id(&self.id) {
            return Err(invalid("tasks.id", "must match `T[0-9]+[A-Z]*`"));
        }
        validate_nonempty("tasks.title", &self.title)?;
        for dependency in &self.depends_on {
            if !is_task_dependency(dependency) {
                return Err(invalid(
                    "tasks.depends_on",
                    "entries must match `T[0-9]+[A-Z]*` or `P[0-9]{2}/T[0-9]+[A-Z]*`",
                ));
            }
        }
        if self.evidence.is_empty() {
            return Err(invalid(
                "tasks.evidence",
                "must contain at least one evidence manifest",
            ));
        }
        for path in &self.evidence {
            validate_repo_path("tasks.evidence", path)?;
        }
        if let Some(red_test) = &self.red_test {
            red_test.validate()?;
        }
        Ok(())
    }
}

/// A task's mutation test that proves a policy fault is detected.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RedTest {
    /// Repository-relative Rust test source.
    pub path: String,
    /// Rust test function name.
    pub name: String,
}

impl RedTest {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_repo_path("tasks.red_test.path", &self.path)?;
        validate_nonempty("tasks.red_test.name", &self.name)
    }
}

/// One deterministic test command for a platform and feature set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TestCommand {
    /// Platform identifier, or `any`.
    pub platform: String,
    /// Cargo features exercised by the command.
    pub features: Vec<String>,
    /// Executable and arguments, without shell interpretation.
    pub argv: Vec<String>,
    /// Must be true for every declared command.
    pub deterministic: bool,
}

impl TestCommand {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("test_commands.platform", &self.platform)?;
        if self.argv.is_empty() || self.argv.iter().any(String::is_empty) {
            return Err(invalid(
                "test_commands.argv",
                "must contain a non-empty executable and non-empty arguments",
            ));
        }
        if !self.deterministic {
            return Err(invalid("test_commands.deterministic", "must be true"));
        }
        Ok(())
    }
}

/// Completeness and feature-gating metadata for a backend.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    /// Runtime backend name.
    pub name: String,
    /// Whether the backend implementation is complete.
    pub complete: bool,
    /// Whether users can select the backend.
    pub selectable: bool,
    /// Cargo feature gating the backend, when one is declared.
    #[serde(default)]
    pub feature: Option<String>,
}

/// A budget manifest referenced by phase metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetReference {
    /// Stable identifier declared inside the budget manifest.
    pub id: String,
    /// Phase that owns the budget.
    pub owner_phase: String,
    /// Repository-relative budget-manifest path.
    pub path: String,
    /// SHA-256 hash of the budget-manifest bytes.
    pub content_hash: String,
}

impl BudgetReference {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("budgets.id", &self.id)?;
        if !is_phase_id(&self.owner_phase) {
            return Err(invalid("budgets.owner_phase", "must match `P[0-9]{2}`"));
        }
        validate_repo_path("budgets.path", &self.path)?;
        validate_hash("budgets.content_hash", &self.content_hash)
    }
}
