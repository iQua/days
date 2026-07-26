//! Direct-dependency and license baseline schema.

use serde::{Deserialize, Serialize};

use super::{SchemaError, SchemaVersion, validate_nonempty};

/// Dependency kinds recorded by the direct-dependency baseline.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DependencyKind {
    /// A runtime dependency.
    Normal,
    /// A test or development dependency.
    Dev,
    /// A build-script dependency.
    Build,
}

/// The offline direct-dependency baseline and resolved-license allowlist.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyBaseline {
    /// Schema version. Only version 1 is accepted.
    pub schema_version: SchemaVersion,
    /// Exact license expressions accepted for resolved packages.
    pub allowed_licenses: Vec<String>,
    /// Direct dependencies declared by workspace members.
    pub direct_dependencies: Vec<DirectDependency>,
}

impl DependencyBaseline {
    pub(super) fn validate(&self) -> Result<(), SchemaError> {
        for license in &self.allowed_licenses {
            validate_nonempty("allowed_licenses", license)?;
        }
        for dependency in &self.direct_dependencies {
            dependency.validate()?;
        }
        Ok(())
    }
}

/// One workspace member's direct dependency declaration.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectDependency {
    /// Workspace package declaring the dependency.
    pub package: String,
    /// Dependency name as declared in the manifest.
    pub name: String,
    /// Version requirement declared in the manifest.
    pub version: String,
    /// Registry, path, or Git source declared in the manifest.
    pub source: String,
    /// Manifest dependency table containing the declaration.
    pub kind: DependencyKind,
}

impl DirectDependency {
    fn validate(&self) -> Result<(), SchemaError> {
        validate_nonempty("direct_dependencies.package", &self.package)?;
        validate_nonempty("direct_dependencies.name", &self.name)?;
        validate_nonempty("direct_dependencies.version", &self.version)?;
        validate_nonempty("direct_dependencies.source", &self.source)
    }
}
