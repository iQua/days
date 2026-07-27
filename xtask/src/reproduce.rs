//! Deterministic execution of phase test commands.

use std::path::Path;
use std::process::{Command, Stdio};

use crate::audit::{
    AuditReport, emit, reproduce_postflight, reproduce_preflight, sort_diagnostics,
};

/// Verifies evidence and runs the commands declared for the host platform.
pub fn reproduce(repo_root: &Path, phase: &str) -> AuditReport {
    let mut report = reproduce_preflight(repo_root, phase);
    if report.has_errors() {
        return report;
    }
    let Some(loaded) = report.phase.as_ref() else {
        return report;
    };
    let metadata = loaded.metadata.clone();

    let host = host_platform_id();
    let mut matched = false;
    for (index, test_command) in metadata.test_commands.iter().enumerate() {
        let subject = format!("{phase} test_commands[{index}]");
        if test_command.platform != "any" && test_command.platform != host {
            emit(
                &mut report.diagnostics,
                "DAYS-AUDIT-0022",
                subject,
                format!(
                    "command for platform {} skipped on {host}",
                    test_command.platform
                ),
            );
            continue;
        }
        matched = true;

        let Some((executable, arguments)) = test_command.argv.split_first() else {
            emit(
                &mut report.diagnostics,
                "DAYS-AUDIT-0023",
                subject,
                "declared test command has no executable",
            );
            break;
        };
        let status = Command::new(executable)
            .args(arguments)
            .current_dir(repo_root)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();
        match status {
            Ok(status) if status.success() => {}
            Ok(status) => {
                emit(
                    &mut report.diagnostics,
                    "DAYS-AUDIT-0023",
                    subject,
                    format!("declared test command exited with {status}"),
                );
                break;
            }
            Err(error) => {
                emit(
                    &mut report.diagnostics,
                    "DAYS-AUDIT-0023",
                    subject,
                    format!("declared test command could not start: {error}"),
                );
                break;
            }
        }
    }

    if !matched {
        emit(
            &mut report.diagnostics,
            "DAYS-AUDIT-0024",
            phase,
            format!("no declared test command matches host platform {host}"),
        );
    }
    report
        .diagnostics
        .extend(reproduce_postflight(repo_root, &metadata));
    sort_diagnostics(&mut report.diagnostics);
    report
}

/// Returns the stable platform identifier for the current host.
pub fn host_platform_id() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}
