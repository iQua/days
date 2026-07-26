//! Versioned TOML schemas used by the audit and evidence contract.

mod baseline;
mod budget;
mod evidence;
mod phase;

use std::fmt;
use std::path::{Component, Path};

pub use baseline::{DependencyBaseline, DependencyKind, DirectDependency};
pub use budget::{BudgetManifest, BudgetThreshold};
pub use evidence::{EvidenceArtifact, EvidenceKind, EvidenceManifest};
pub use phase::{Backend, BudgetReference, PhaseMetadata, PhaseTask, RedTest, TestCommand};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::hash::is_sha256;

/// The only schema version supported by this implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaVersion(u32);

impl SchemaVersion {
    /// Schema version 1.
    pub const V1: Self = Self(1);

    /// Returns the integer form stored in TOML.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Serialize for SchemaVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SchemaVersionVisitor;

        impl Visitor<'_> for SchemaVersionVisitor {
            type Value = SchemaVersion;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("integer schema_version = 1")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == i64::from(SchemaVersion::V1.get()) {
                    Ok(SchemaVersion::V1)
                } else {
                    Err(E::custom(format!("unsupported schema_version {value}")))
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == u64::from(SchemaVersion::V1.get()) {
                    Ok(SchemaVersion::V1)
                } else {
                    Err(E::custom(format!("unsupported schema_version {value}")))
                }
            }
        }

        deserializer.deserialize_u32(SchemaVersionVisitor)
    }
}

/// A schema parsing or semantic-validation failure.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// The document declares a schema version this implementation cannot read.
    #[error("unsupported schema_version {found}; expected 1")]
    UnsupportedVersion {
        /// The unsupported integer found in the document.
        found: i64,
    },
    /// The document is not valid TOML or does not match the schema shape.
    #[error("malformed metadata: {0}")]
    Parse(#[from] toml::de::Error),
    /// The document shape is valid but a field value violates the schema.
    #[error("malformed metadata: {message}")]
    Validation {
        /// Description of the invalid field value.
        message: String,
    },
    /// An evidence manifest records a budget hash without naming its budget.
    #[error("malformed metadata: `budget_hash` must not be present unless `budget` is present")]
    BudgetHashWithoutBudget,
}

/// Parses and validates phase metadata.
pub fn parse_phase_metadata(input: &str) -> Result<PhaseMetadata, SchemaError> {
    parse_validated(input, PhaseMetadata::validate)
}

/// Parses and validates an evidence manifest.
pub fn parse_evidence_manifest(input: &str) -> Result<EvidenceManifest, SchemaError> {
    parse_validated(input, EvidenceManifest::validate)
}

/// Parses and validates a budget manifest.
pub fn parse_budget_manifest(input: &str) -> Result<BudgetManifest, SchemaError> {
    parse_validated(input, BudgetManifest::validate)
}

/// Parses and validates the direct-dependency and license baseline.
pub fn parse_dependency_baseline(input: &str) -> Result<DependencyBaseline, SchemaError> {
    parse_validated(input, DependencyBaseline::validate)
}

fn parse_validated<T>(
    input: &str,
    validate: impl FnOnce(&T) -> Result<(), SchemaError>,
) -> Result<T, SchemaError>
where
    T: for<'de> Deserialize<'de>,
{
    reject_unsupported_version(input)?;
    let document = toml::from_str(input)?;
    validate(&document)?;
    Ok(document)
}

fn reject_unsupported_version(input: &str) -> Result<(), SchemaError> {
    let document: toml::Value = toml::from_str(input)?;
    if let Some(version) = document
        .get("schema_version")
        .and_then(toml::Value::as_integer)
    {
        if version != i64::from(SchemaVersion::V1.get()) {
            return Err(SchemaError::UnsupportedVersion { found: version });
        }
    }
    Ok(())
}

fn validate_hash(field: &str, value: &str) -> Result<(), SchemaError> {
    if is_sha256(value) {
        Ok(())
    } else {
        Err(invalid(
            field,
            "must be `sha256:` followed by 64 lowercase hexadecimal characters",
        ))
    }
}

fn validate_git_commit(field: &str, value: &str) -> Result<(), SchemaError> {
    if value.len() == 40
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        Ok(())
    } else {
        Err(invalid(
            field,
            "must be exactly 40 lowercase hexadecimal characters",
        ))
    }
}

fn validate_nonempty(field: &str, value: &str) -> Result<(), SchemaError> {
    if value.is_empty() {
        Err(invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_repo_path(field: &str, value: &str) -> Result<(), SchemaError> {
    validate_nonempty(field, value)?;
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(invalid(
            field,
            "must be a repository-relative path without parent traversal",
        ));
    }
    Ok(())
}

fn invalid(field: &str, detail: &str) -> SchemaError {
    SchemaError::Validation {
        message: format!("`{field}` {detail}"),
    }
}

fn is_phase_id(value: &str) -> bool {
    value.len() == 3
        && value.starts_with('P')
        && value.as_bytes()[1..].iter().all(u8::is_ascii_digit)
}

fn is_task_id(value: &str) -> bool {
    let Some(suffix) = value.strip_prefix('T') else {
        return false;
    };
    let digit_count = suffix.bytes().take_while(u8::is_ascii_digit).count();

    digit_count > 0
        && suffix.as_bytes()[digit_count..]
            .iter()
            .all(u8::is_ascii_uppercase)
}

fn is_task_dependency(value: &str) -> bool {
    if is_task_id(value) {
        return true;
    }

    value
        .split_once('/')
        .is_some_and(|(phase, task)| is_phase_id(phase) && is_task_id(task))
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::{SchemaVersion, is_phase_id, is_task_dependency, is_task_id};

    #[test]
    fn identifier_syntax_is_strict() {
        assert!(is_phase_id("P01"));
        assert!(!is_phase_id("P1"));
        assert!(!is_phase_id("p01"));
        assert!(is_task_id("T0"));
        assert!(is_task_id("T123"));
        for task in [
            "T3R", "T8B", "T9A", "T12M", "T12R", "T13M", "T15M", "T16M", "T17A", "T17B", "T17P",
            "T17Q", "T17S", "T17V", "T18A", "T18B", "T18C", "T18P", "T21A", "T21B",
        ] {
            assert!(is_task_id(task), "{task} must be accepted");
        }
        for task in [
            "", "T", "TR", "T3r", "t3R", "T 3", "T3 R", "T3-R", "T3_R", "T3!", "T٣R",
        ] {
            assert!(!is_task_id(task), "{task:?} must be rejected");
        }
        assert!(is_task_dependency("P01/T3"));
        assert!(is_task_dependency("P03/T3R"));
        assert!(!is_task_dependency("P1/T3"));
    }

    #[test]
    fn schema_version_serializes_as_an_integer() {
        #[derive(Serialize)]
        struct Document {
            schema_version: SchemaVersion,
        }

        assert_eq!(
            toml::to_string(&Document {
                schema_version: SchemaVersion::V1,
            })
            .expect("serialize"),
            "schema_version = 1\n"
        );
    }
}
