//! Performance-budget manifest schema.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::{
    SchemaError, SchemaVersion, invalid, is_phase_id, validate_hash, validate_nonempty,
    validate_repo_file, validate_repo_path,
};
use crate::hash::sha256_file;

/// Required policy text for every budget waiver.
pub const REQUIRED_WAIVER_POLICY: &str =
    "A waiver must be a reviewed manifest change made before the cutover decision.";

/// A frozen set of admission or default-selection thresholds.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetManifest {
    /// Schema version. Only version 2 is accepted.
    pub schema_version: SchemaVersion,
    /// Stable budget identifier.
    pub id: String,
    /// Phase that owns this budget.
    pub phase: String,
    /// ISO-8601 date on which the budget was frozen.
    pub frozen_at: String,
    /// Human-readable budget purpose.
    pub description: String,
    /// Named measurement platform and toolchain.
    pub platform: BudgetPlatform,
    /// Frozen measurement method.
    pub method: BudgetMethod,
    /// Workloads and comparison boundaries covered by this budget.
    pub corpus: Vec<BudgetCorpusEntry>,
    /// Thresholds frozen before measurement.
    pub thresholds: Vec<BudgetThreshold>,
    /// Authority and process required to waive a threshold.
    pub waiver: BudgetWaiver,
}

impl BudgetManifest {
    pub(super) fn validate(&self, repo_root: &Path) -> Result<(), SchemaError> {
        validate_nonempty("id", &self.id)?;
        if !is_phase_id(&self.phase) {
            return Err(invalid("phase", "must match `P[0-9]{2}`"));
        }
        if !is_iso_date(&self.frozen_at) {
            return Err(invalid(
                "frozen_at",
                "must be a valid date in `YYYY-MM-DD` form",
            ));
        }
        validate_nonempty("description", &self.description)?;
        self.platform.validate()?;
        self.method.validate()?;
        if self.corpus.is_empty() {
            return Err(invalid("corpus", "must contain at least one entry"));
        }
        for entry in &self.corpus {
            entry.validate(repo_root)?;
        }
        if self.thresholds.is_empty() {
            return Err(invalid("thresholds", "must contain at least one threshold"));
        }
        for threshold in &self.thresholds {
            threshold.validate()?;
        }
        self.waiver.validate()?;
        Ok(())
    }
}

/// Named hardware, operating system, and compiler toolchain.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetPlatform {
    /// Stable name for the measurement host.
    pub name: String,
    /// CPU or system-on-chip model.
    pub cpu: String,
    /// Operating-system version and build.
    pub os_build: String,
    /// Compiler and package-manager toolchain.
    pub toolchain: String,
}

impl BudgetPlatform {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("platform.name", &self.name)?;
        validate_nonempty("platform.cpu", &self.cpu)?;
        validate_nonempty("platform.os_build", &self.os_build)?;
        validate_nonempty("platform.toolchain", &self.toolchain)
    }
}

/// Frozen benchmark sampling and analysis method.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetMethod {
    /// Unmeasured warmup iterations.
    pub warmups: u32,
    /// Measured repetitions per corpus entry.
    pub repetitions: u32,
    /// Statistic computed from the repetitions.
    pub statistic: String,
    /// Confidence or noise-tolerance rule.
    pub confidence_rule: String,
}

impl BudgetMethod {
    fn validate(&self) -> Result<(), SchemaError> {
        if self.repetitions < 1 {
            return Err(invalid("method.repetitions", "must be at least 1"));
        }
        validate_nonempty("method.statistic", &self.statistic)?;
        validate_nonempty("method.confidence_rule", &self.confidence_rule)
    }
}

/// One corpus input and the boundary at which it is compared.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetCorpusEntry {
    /// Repository-relative path to the exact workload configuration.
    pub path: String,
    /// SHA-256 of the exact file bytes.
    pub content_hash: String,
    /// Strongest comparison admitted for this workload.
    pub comparison_boundary: ComparisonBoundary,
}

impl BudgetCorpusEntry {
    fn validate(&self, repo_root: &Path) -> Result<(), SchemaError> {
        validate_repo_path("corpus.path", &self.path)?;
        validate_hash("corpus.content_hash", &self.content_hash)?;
        let path = validate_repo_file(repo_root, "corpus.path", &self.path)?;
        let actual = sha256_file(&path).map_err(|error| {
            invalid(
                "corpus.path",
                &format!("cannot be hashed as a regular file: {error}"),
            )
        })?;
        if actual != self.content_hash {
            return Err(invalid(
                "corpus.content_hash",
                &format!("does not match file `{}` (actual {actual})", self.path),
            ));
        }
        Ok(())
    }
}

/// Comparison strength frozen for a corpus entry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ComparisonBoundary {
    /// Compare the complete transition ledger.
    ExactLedger,
    /// Compare only canonical terminal observations.
    TerminalObservation,
    /// Record a versioned semantic migration rather than full-key equality.
    SemanticMigration,
}

/// One named threshold in a budget manifest.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetThreshold {
    /// Stable threshold name.
    pub name: String,
    /// Metric being bounded.
    pub metric: String,
    /// Comparison operator.
    pub comparison: String,
    /// Numeric comparison value.
    pub value: f64,
    /// Unit in which `value` is expressed.
    pub unit: String,
}

impl BudgetThreshold {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("thresholds.name", &self.name)?;
        validate_nonempty("thresholds.metric", &self.metric)?;
        if !matches!(self.comparison.as_str(), "<" | "<=" | ">" | ">=" | "==") {
            return Err(invalid(
                "thresholds.comparison",
                "must be one of `<`, `<=`, `>`, `>=`, or `==`",
            ));
        }
        if !self.value.is_finite() {
            return Err(invalid("thresholds.value", "must be finite"));
        }
        validate_nonempty("thresholds.unit", &self.unit)
    }
}

/// Reviewed authority and policy for threshold exceptions.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetWaiver {
    /// Public role authorized to approve a waiver.
    pub approving_role: String,
    /// Required review timing for a waiver.
    pub policy: String,
}

impl BudgetWaiver {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("waiver.approving_role", &self.approving_role)?;
        if self.policy != REQUIRED_WAIVER_POLICY {
            return Err(invalid(
                "waiver.policy",
                &format!("must equal `{REQUIRED_WAIVER_POLICY}`"),
            ));
        }
        Ok(())
    }
}

fn is_iso_date(value: &str) -> bool {
    if value.len() != 10
        || value.as_bytes()[4] != b'-'
        || value.as_bytes()[7] != b'-'
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
    {
        return false;
    }

    let year = value[0..4].parse::<u32>().ok();
    let month = value[5..7].parse::<u32>().ok();
    let day = value[8..10].parse::<u32>().ok();
    let (Some(year), Some(month), Some(day)) = (year, month, day) else {
        return false;
    };
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }

    let days_in_month = match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=days_in_month).contains(&day)
}

fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

#[cfg(test)]
mod tests {
    use super::is_iso_date;

    #[test]
    fn validates_calendar_dates() {
        assert!(is_iso_date("2024-02-29"));
        assert!(!is_iso_date("2023-02-29"));
        assert!(!is_iso_date("2026-13-01"));
        assert!(!is_iso_date("26-07-01"));
    }
}
