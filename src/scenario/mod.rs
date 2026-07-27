//! Backend-neutral lowering from supported Days configuration into one semantic image.

mod compile;
mod ids;

pub use compile::{CompileError, compile_config};
