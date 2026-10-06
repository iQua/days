//! The adapter's one error type: a refusal with the trace line it concerns, when there is one.

use std::fmt;

/// A refusal: the input is not something the adapter lowers, with the reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AicbError {
    /// 1-based line of the trace (or of `SimAI.conf`) the refusal concerns.
    pub line: Option<usize>,
    pub message: String,
}

impl AicbError {
    pub(crate) fn at(line: usize, message: impl Into<String>) -> Self {
        Self {
            line: Some(line),
            message: message.into(),
        }
    }

    #[allow(dead_code)] // used by the group and schedule modules
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            line: None,
            message: message.into(),
        }
    }
}

impl fmt::Display for AicbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(line) => write!(f, "line {line}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for AicbError {}
