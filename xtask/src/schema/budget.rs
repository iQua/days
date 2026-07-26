//! Performance-budget manifest schema.

use serde::{Deserialize, Serialize};

use super::{SchemaError, SchemaVersion, invalid, is_phase_id, validate_nonempty};

/// A frozen set of admission or default-selection thresholds.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetManifest {
    /// Schema version. Only version 1 is accepted.
    pub schema_version: SchemaVersion,
    /// Stable budget identifier.
    pub id: String,
    /// Phase that owns this budget.
    pub phase: String,
    /// ISO-8601 date on which the budget was frozen.
    pub frozen_at: String,
    /// Human-readable budget purpose.
    pub description: String,
    /// Thresholds frozen before measurement.
    pub thresholds: Vec<BudgetThreshold>,
}

impl BudgetManifest {
    pub(super) fn validate(&self) -> Result<(), SchemaError> {
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
        if self.thresholds.is_empty() {
            return Err(invalid("thresholds", "must contain at least one threshold"));
        }
        for threshold in &self.thresholds {
            threshold.validate()?;
        }
        Ok(())
    }
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
