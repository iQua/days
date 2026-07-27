//! Performance-budget manifest schema.

use std::collections::BTreeSet;
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
    /// Schema version. Only version 4 is accepted.
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
    /// Admission statistic and thresholds evaluated after baseline collection.
    pub admission: BudgetAdmission,
    /// Workloads and comparison boundaries covered by this budget.
    pub corpus: Vec<BudgetCorpusEntry>,
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
        self.admission.validate(&self.corpus)?;
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
    /// CPU count expected on the named measurement host.
    pub expected_num_cpus: u32,
    /// Source used to determine the multi-threaded worker count.
    pub mt_thread_count_source: String,
}

impl BudgetPlatform {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("platform.name", &self.name)?;
        validate_nonempty("platform.cpu", &self.cpu)?;
        validate_nonempty("platform.os_build", &self.os_build)?;
        validate_nonempty("platform.toolchain", &self.toolchain)?;
        if self.expected_num_cpus < 1 {
            return Err(invalid("platform.expected_num_cpus", "must be at least 1"));
        }
        validate_nonempty(
            "platform.mt_thread_count_source",
            &self.mt_thread_count_source,
        )
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
    /// Cargo build profile used for every measurement binary.
    pub build_profile: String,
    /// Exact Cargo flags used to build and run the measurement binary.
    pub cargo_flags: Vec<String>,
    /// Primary wall-time boundary around simulator execution only.
    pub sim_execution_boundary: String,
    /// Secondary wall-time boundary spanning the full process.
    pub end_to_end_boundary: String,
    /// Minimum acceptable observed wall time for a measured sample.
    pub minimum_sample_wall_time_seconds: f64,
    /// Effective simulated duration required for each sample.
    pub effective_simulation_duration_seconds: f64,
    /// Rule used to resolve the effective simulated duration.
    pub effective_simulation_duration_source: String,
    /// Rule validating the simulated end time of a sample.
    pub sample_simulated_end_time_rule: String,
    /// Rule validating the effective thread count of a sample.
    pub sample_effective_thread_count_rule: String,
    /// Order in which corpus entries and modes are run.
    pub run_order: String,
    /// Rule for pairing repetitions across compared modes.
    pub pairing_order: String,
    /// Deterministic confidence-interval resampling algorithm.
    pub resampling_algorithm: String,
    /// Pseudorandom number generator used by resampling.
    pub resampling_prng: String,
    /// Fixed resampling seed.
    pub resampling_seed: u64,
    /// Stable name for the single-threaded Nexosim mode.
    pub st_mode: String,
    /// Stable name for the multi-threaded Nexosim mode.
    pub mt_mode: String,
    /// Rule selecting the best exact Nexosim CPU mode.
    pub best_exact_mode_rule: String,
    /// Implicit configuration values that affect the declared measurement.
    pub resolved_defaults: Vec<BudgetResolvedDefault>,
}

impl BudgetMethod {
    fn validate(&self) -> Result<(), SchemaError> {
        if self.repetitions < 1 {
            return Err(invalid("method.repetitions", "must be at least 1"));
        }
        validate_nonempty("method.build_profile", &self.build_profile)?;
        if self.cargo_flags.is_empty() {
            return Err(invalid(
                "method.cargo_flags",
                "must contain at least one flag",
            ));
        }
        for flag in &self.cargo_flags {
            validate_nonempty("method.cargo_flags", flag)?;
        }
        validate_nonempty(
            "method.sim_execution_boundary",
            &self.sim_execution_boundary,
        )?;
        validate_nonempty("method.end_to_end_boundary", &self.end_to_end_boundary)?;
        validate_positive_finite(
            "method.minimum_sample_wall_time_seconds",
            self.minimum_sample_wall_time_seconds,
        )?;
        validate_positive_finite(
            "method.effective_simulation_duration_seconds",
            self.effective_simulation_duration_seconds,
        )?;
        validate_nonempty(
            "method.effective_simulation_duration_source",
            &self.effective_simulation_duration_source,
        )?;
        validate_nonempty(
            "method.sample_simulated_end_time_rule",
            &self.sample_simulated_end_time_rule,
        )?;
        validate_nonempty(
            "method.sample_effective_thread_count_rule",
            &self.sample_effective_thread_count_rule,
        )?;
        validate_nonempty("method.run_order", &self.run_order)?;
        validate_nonempty("method.pairing_order", &self.pairing_order)?;
        validate_nonempty("method.resampling_algorithm", &self.resampling_algorithm)?;
        validate_nonempty("method.resampling_prng", &self.resampling_prng)?;
        validate_nonempty("method.st_mode", &self.st_mode)?;
        validate_nonempty("method.mt_mode", &self.mt_mode)?;
        validate_nonempty("method.best_exact_mode_rule", &self.best_exact_mode_rule)?;
        if self.resolved_defaults.is_empty() {
            return Err(invalid(
                "method.resolved_defaults",
                "must contain at least one resolved default",
            ));
        }
        let mut names = BTreeSet::new();
        for default in &self.resolved_defaults {
            default.validate()?;
            if !names.insert(&default.name) {
                return Err(invalid(
                    "method.resolved_defaults.name",
                    &format!("duplicate resolved default `{}`", default.name),
                ));
            }
        }
        Ok(())
    }
}

/// One configuration input inherited from production code rather than corpus bytes.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetResolvedDefault {
    /// Stable name of the implicit input.
    pub name: String,
    /// Exact resolved value and unit.
    pub value: String,
    /// Production source location and expression.
    pub source: String,
}

impl BudgetResolvedDefault {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("method.resolved_defaults.name", &self.name)?;
        validate_nonempty("method.resolved_defaults.value", &self.value)?;
        validate_nonempty("method.resolved_defaults.source", &self.source)
    }
}

/// Admission statistic and thresholds evaluated at the retirement decision.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetAdmission {
    /// Phase at which admission is evaluated.
    pub evaluated_at: String,
    /// Statistic computed from paired candidate and baseline measurements.
    pub statistic: String,
    /// Confidence or noise-tolerance rule.
    pub confidence_rule: String,
    /// Thresholds frozen before baseline measurement.
    pub thresholds: Vec<BudgetThreshold>,
}

impl BudgetAdmission {
    fn validate(&self, corpus: &[BudgetCorpusEntry]) -> Result<(), SchemaError> {
        if self.evaluated_at != "P23" {
            return Err(invalid("admission.evaluated_at", "must equal `P23`"));
        }
        validate_nonempty("admission.statistic", &self.statistic)?;
        validate_nonempty("admission.confidence_rule", &self.confidence_rule)?;
        if self.thresholds.is_empty() {
            return Err(invalid(
                "admission.thresholds",
                "must contain at least one threshold",
            ));
        }
        for threshold in &self.thresholds {
            threshold.validate(corpus)?;
        }
        Ok(())
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
    /// Stable workload identity independent of execution mode.
    pub workload: String,
    /// Stable execution-mode identity.
    pub mode: String,
    /// Purpose of this corpus entry.
    pub role: CorpusRole,
}

impl BudgetCorpusEntry {
    fn validate(&self, repo_root: &Path) -> Result<(), SchemaError> {
        validate_nonempty("corpus.workload", &self.workload)?;
        validate_nonempty("corpus.mode", &self.mode)?;
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

/// Purpose of one frozen corpus entry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CorpusRole {
    /// Correctness characterization, including ledger or terminal comparison.
    Correctness,
    /// Performance baseline or admission measurement.
    Performance,
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
    /// Machine-readable class used to validate timing-boundary requirements.
    pub metric_kind: ThresholdMetricKind,
    /// Corpus-level scope or the stable identifier of one workload.
    pub applies_to: String,
    /// Wall-time scope used by a timing metric.
    pub timing_boundary: Option<TimingBoundary>,
    /// Comparison operator.
    pub comparison: String,
    /// Numeric comparison value.
    pub value: f64,
    /// Unit in which `value` is expressed.
    pub unit: String,
}

impl BudgetThreshold {
    fn validate(&self, corpus: &[BudgetCorpusEntry]) -> Result<(), SchemaError> {
        validate_nonempty("admission.thresholds.name", &self.name)?;
        validate_nonempty("admission.thresholds.metric", &self.metric)?;
        validate_nonempty("admission.thresholds.applies_to", &self.applies_to)?;
        if self.applies_to != "corpus"
            && !corpus.iter().any(|entry| entry.workload == self.applies_to)
        {
            return Err(invalid(
                "admission.thresholds.applies_to",
                &format!(
                    "must equal `corpus` or name a workload present in the corpus; found `{}`",
                    self.applies_to
                ),
            ));
        }
        match (self.metric_kind.is_timing(), self.timing_boundary) {
            (true, None) => {
                return Err(invalid(
                    "admission.thresholds.timing_boundary",
                    "is required for wall-time and throughput metrics",
                ));
            }
            (false, Some(_)) => {
                return Err(invalid(
                    "admission.thresholds.timing_boundary",
                    "must be absent for byte and count metrics",
                ));
            }
            _ => {}
        }
        if !matches!(self.comparison.as_str(), "<" | "<=" | ">" | ">=" | "==") {
            return Err(invalid(
                "admission.thresholds.comparison",
                "must be one of `<`, `<=`, `>`, `>=`, or `==`",
            ));
        }
        if !self.value.is_finite() {
            return Err(invalid("admission.thresholds.value", "must be finite"));
        }
        validate_nonempty("admission.thresholds.unit", &self.unit)
    }
}

/// Machine-readable threshold metric class.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ThresholdMetricKind {
    /// Elapsed wall-clock duration, including compile latency.
    WallTime,
    /// Work completed per unit of wall-clock duration.
    Throughput,
    /// Byte-valued memory or storage quantity.
    Bytes,
    /// Integer-valued capacity or cardinality.
    Count,
}

impl ThresholdMetricKind {
    fn is_timing(self) -> bool {
        matches!(self, Self::WallTime | Self::Throughput)
    }
}

/// Wall-time scope selected by one timing threshold.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingBoundary {
    /// Simulator execution loop only.
    SimExecution,
    /// Whole declared measurement command.
    EndToEnd,
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

fn validate_positive_finite(field: &str, value: f64) -> Result<(), SchemaError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(invalid(field, "must be positive and finite"))
    }
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
