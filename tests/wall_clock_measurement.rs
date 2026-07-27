use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::fs;

#[test]
fn reports_step_until_wall_clock_time_separately() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_path = tmp.path().join("logs");
    let config_path = tmp.path().join("simple.toml");
    let config = fs::read_to_string("configs/simple.toml")
        .expect("read simple config")
        .replace(
            "log_path = \"logs/simple\"",
            &format!("log_path = \"{}\"", log_path.display()),
        );
    fs::write(&config_path, config).expect("write temporary config");

    let mut cmd = cargo_bin_cmd!("days");
    cmd.arg(config_path);

    cmd.assert()
        .success()
        .stderr(predicate::str::contains(
            "Nexosim step_until wall-clock time:",
        ))
        .stderr(predicate::str::contains("Nexosim total wall-clock time:"))
        .stderr(predicate::str::contains("Elapsed wall-clock time:"));
}
