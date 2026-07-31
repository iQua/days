use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::fs;
use std::io::Write;

#[test]
fn leanguard_run_check_only_uses_manifest_and_runs_checkers() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_path = tmp.path().join("logs");
    fs::create_dir_all(&log_path).expect("create log dir");

    let checker_dir = tmp.path().join("checkers");
    fs::create_dir_all(&checker_dir).expect("create checker dir");

    // Minimal config surface for leanguard-run: log_path + threading.
    let config_path = tmp.path().join("case.toml");
    fs::write(
        &config_path,
        format!(
            "log_path = \"{}\"\nthreading = \"single\"\n",
            log_path.display()
        ),
    )
    .expect("write config");

    // Create manifest + dummy trace files (non-empty).
    fs::write(
        log_path.join("traces.json"),
        r#"{"version":1,"traces":["aqm_events.csv","dcqcn_events.csv"]}"#,
    )
    .expect("write manifest");
    fs::write(log_path.join("aqm_events.csv"), "x").expect("write aqm_events.csv");
    fs::write(log_path.join("dcqcn_events.csv"), "y").expect("write dcqcn_events.csv");

    // Create stub checker executables.
    for exe in ["aqm_check", "dcqcn_check", "aqm_dcqcn_check"] {
        let path = checker_dir.join(exe);
        let mut f = fs::File::create(&path).expect("create stub checker");
        writeln!(f, "#!/bin/sh").unwrap();
        writeln!(f, "echo ACCEPT").unwrap();
        writeln!(f, "exit 0").unwrap();
        drop(f);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&path, perms).unwrap();
        }
    }

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"accept\": true"))
        .stdout(predicate::str::contains("\"checker_results\""))
        .stdout(predicate::str::contains("aqm_check"))
        .stdout(predicate::str::contains("dcqcn_check"))
        .stdout(predicate::str::contains("aqm_dcqcn_check"));
}

#[test]
fn leanguard_run_scan_fallback_discovers_executor_tcp_trace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_path = tmp.path().join("logs");
    let checker_dir = tmp.path().join("checkers");
    fs::create_dir_all(&log_path).expect("create log dir");
    fs::create_dir_all(&checker_dir).expect("create checker dir");
    let config_path = tmp.path().join("case.toml");
    fs::write(
        &config_path,
        format!(
            "log_path = \"{}\"\nthreading = \"single\"\n",
            log_path.display()
        ),
    )
    .expect("write config");
    fs::write(log_path.join("tcp_events.csv"), "nonempty").expect("write executor TCP trace");
    let checker = checker_dir.join("tcp_check");
    fs::write(&checker, "#!/bin/sh\necho ACCEPT\nexit 0\n").expect("write checker");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&checker).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&checker, permissions).unwrap();
    }

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
    ]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("tcp_check"))
        .stdout(predicate::str::contains("\"accept\": true"));
}
