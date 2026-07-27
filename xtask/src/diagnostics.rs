//! Stable diagnostics emitted by the Days Executor audit.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The severity of a diagnostic.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// A fault that makes the audit fail.
    Error,
    /// An informational message that does not affect the exit status.
    Info,
}

impl fmt::Display for Severity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error => formatter.write_str("error"),
            Self::Info => formatter.write_str("info"),
        }
    }
}

/// One entry in the stable diagnostic registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DiagnosticDefinition {
    /// Stable diagnostic code.
    pub code: &'static str,
    /// Stable machine-readable slug.
    pub slug: &'static str,
    /// Whether the diagnostic is an error or informational.
    pub severity: Severity,
    /// Short description of the condition.
    pub summary: &'static str,
}

/// The complete stable diagnostic registry.
pub static REGISTRY: &[DiagnosticDefinition] = &[
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0001",
        slug: "phase-metadata-missing",
        severity: Severity::Error,
        summary: "No `docs/days-executor/phases/<PHASE>-*.toml`, or more than one match",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0002",
        slug: "phase-metadata-malformed",
        severity: Severity::Error,
        summary: "Metadata is not valid TOML, has an unknown field, a missing required field, or a malformed value (bad phase id, bad hash format, bad date, `deterministic = false`)",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0003",
        slug: "phase-metadata-schema-version",
        severity: Severity::Error,
        summary: "Unsupported `schema_version` in any versioned schema",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0004",
        slug: "phase-metadata-identity",
        severity: Severity::Error,
        summary: "`phase`/`name` disagree with the file stem, or a duplicate phase id or duplicate task id exists across the metadata set",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0005",
        slug: "unrecorded-dependency",
        severity: Severity::Error,
        summary: "A dependency is absent, has an unknown external gate, or lacks phase closure",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0006",
        slug: "dependency-cycle",
        severity: Severity::Error,
        summary: "The phase/task dependency graph contains a cycle",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0007",
        slug: "red-test-missing",
        severity: Severity::Error,
        summary: "A task declares no red test, or its `path` does not exist, or the file contains no `fn <name>`",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0008",
        slug: "forbidden-source",
        severity: Severity::Error,
        summary: "Hand-authored/checked-in forbidden source or interpreter in the audited tree",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0009",
        slug: "forbidden-toolchain",
        severity: Severity::Error,
        summary: "An executor-owned workflow or declared command invokes a forbidden tool",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0010",
        slug: "generated-tree-dirty",
        severity: Severity::Error,
        summary: "A generated/inspection directory is not gitignored, has tracked files, or is dirty",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0011",
        slug: "budget-hash-mismatch",
        severity: Severity::Error,
        summary: "A declared budget manifest is missing, or its `content_hash` in the phase metadata does not match the file",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0012",
        slug: "post-measurement-budget-change",
        severity: Severity::Error,
        summary: "An evidence record's `budget_hash` does not match the current hash of the budget it cites",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0013",
        slug: "evidence-manifest-invalid",
        severity: Severity::Error,
        summary: "An evidence manifest is missing, unparseable, or schema-invalid",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0014",
        slug: "evidence-checksum-mismatch",
        severity: Severity::Error,
        summary: "A checked-in golden artifact is missing or its content hash does not match",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0015",
        slug: "evidence-link-not-immutable",
        severity: Severity::Error,
        summary: "A days-gpu archive artifact is missing required immutable provenance or does not match its recorded commit",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0016",
        slug: "incomplete-backend-selectable",
        severity: Severity::Error,
        summary: "A backend marked incomplete is selectable or lacks a feature gate",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0017",
        slug: "matrix-command-missing",
        severity: Severity::Error,
        summary: "A declared feature or platform has no corresponding declared test command",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0018",
        slug: "design-note-missing",
        severity: Severity::Error,
        summary: "The declared design note does not exist",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0019",
        slug: "dependency-baseline-drift",
        severity: Severity::Error,
        summary: "The workspace's direct dependencies differ from `dependency-baseline.toml`",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0020",
        slug: "license-not-allowed",
        severity: Severity::Error,
        summary: "A package in the resolved dependency set has a license outside `allowed_licenses`",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0021",
        slug: "proof-evidence-missing",
        severity: Severity::Error,
        summary: "`proof_changing = true` but a required proof-evidence tag is absent",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0022",
        slug: "optional-check-skipped",
        severity: Severity::Info,
        summary: "An optional check that needs the network or an absent tool was skipped",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0023",
        slug: "reproduce-command-failed",
        severity: Severity::Error,
        summary: "(reproduce only) A declared test command exited non-zero",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0024",
        slug: "reproduce-no-host-command",
        severity: Severity::Error,
        summary: "(reproduce only) No declared test command matches the host platform",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0025",
        slug: "audit-internal-error",
        severity: Severity::Error,
        summary: "An audit check attempted to emit an unknown diagnostic code",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0026",
        slug: "budget-freeze-invalid",
        severity: Severity::Error,
        summary: "A budget freeze commit is missing or unverifiable, contains different budget content, or does not strictly precede its measurement commit",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0027",
        slug: "measurement-evidence-invalid",
        severity: Severity::Error,
        summary: "Measurement evidence omits its required budget, budget hash, or run commit, or a declared budget has no citing measurement",
    },
    DiagnosticDefinition {
        code: "DAYS-AUDIT-0028",
        slug: "archive-check-skipped",
        severity: Severity::Info,
        summary: "Archive verification was skipped because the days-gpu repository is unavailable",
    },
];

/// Error returned when constructing a diagnostic with an unknown code.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("unknown diagnostic code `{code}`")]
pub struct UnknownDiagnosticCode {
    code: String,
}

/// A concrete diagnostic produced by an audit check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    definition: &'static DiagnosticDefinition,
    subject: String,
    message: String,
}

impl Diagnostic {
    /// Constructs a diagnostic by looking up `code` in the stable registry.
    pub fn new(
        code: &str,
        subject: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Self, UnknownDiagnosticCode> {
        let definition = definition(code).ok_or_else(|| UnknownDiagnosticCode {
            code: code.to_owned(),
        })?;
        Ok(Self::from_definition(definition, subject, message))
    }

    /// Constructs a diagnostic from a registry definition.
    pub fn from_definition(
        definition: &'static DiagnosticDefinition,
        subject: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            definition,
            subject: subject.into(),
            message: message.into(),
        }
    }

    /// Returns the stable diagnostic code.
    pub fn code(&self) -> &'static str {
        self.definition.code
    }

    /// Returns the stable diagnostic slug.
    pub fn slug(&self) -> &'static str {
        self.definition.slug
    }

    /// Returns the diagnostic severity.
    pub fn severity(&self) -> Severity {
        self.definition.severity
    }

    /// Returns whether this diagnostic makes the audit fail.
    pub fn is_error(&self) -> bool {
        self.definition.severity == Severity::Error
    }

    /// Returns the diagnostic subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {} ",
            self.definition.code, self.definition.slug
        )?;
        write_single_line(formatter, &self.subject)?;
        formatter.write_str(": ")?;
        write_single_line(formatter, &self.message)
    }
}

fn write_single_line(formatter: &mut fmt::Formatter<'_>, value: &str) -> fmt::Result {
    for character in value.chars() {
        match character {
            '\n' => formatter.write_str("\\n")?,
            '\r' => formatter.write_str("\\r")?,
            '\t' => formatter.write_str("\\t")?,
            control if control.is_control() => {
                write!(formatter, "\\u{{{:x}}}", u32::from(control))?;
            }
            printable => write!(formatter, "{printable}")?,
        }
    }
    Ok(())
}

/// Looks up a diagnostic definition by its stable code.
pub fn definition(code: &str) -> Option<&'static DiagnosticDefinition> {
    REGISTRY.iter().find(|entry| entry.code == code)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{Diagnostic, REGISTRY};

    #[test]
    fn registry_codes_and_slugs_are_unique() {
        let mut codes = BTreeSet::new();
        let mut slugs = BTreeSet::new();

        for diagnostic in REGISTRY {
            assert!(codes.insert(diagnostic.code), "{}", diagnostic.code);
            assert!(slugs.insert(diagnostic.slug), "{}", diagnostic.slug);
        }
    }

    #[test]
    fn diagnostic_display_is_stable() {
        let diagnostic = Diagnostic::new(
            "DAYS-AUDIT-0008",
            "executor/kernel.cu",
            "hand-authored `.cu` source in an executor-owned path",
        )
        .expect("registered diagnostic");

        assert_eq!(
            diagnostic.to_string(),
            "DAYS-AUDIT-0008 forbidden-source executor/kernel.cu: \
             hand-authored `.cu` source in an executor-owned path"
        );
    }

    #[test]
    fn diagnostic_display_escapes_physical_line_breaks() {
        let diagnostic = Diagnostic::new(
            "DAYS-AUDIT-0002",
            "metadata\nfile",
            "parse failed\r\non line 2",
        )
        .expect("registered diagnostic");

        assert_eq!(
            diagnostic.to_string(),
            "DAYS-AUDIT-0002 phase-metadata-malformed metadata\\nfile: \
             parse failed\\r\\non line 2"
        );
        assert_eq!(diagnostic.to_string().lines().count(), 1);
    }
}
