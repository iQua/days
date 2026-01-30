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
    // aqm_events.csv must be a valid CSV for NDJSON/TLA export when TLC baseline is enabled.
    fs::write(log_path.join("aqm_events.csv"), "time_ns,event_id\n1,0\n")
        .expect("write aqm_events.csv");
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
fn leanguard_run_check_only_can_run_tlc_via_stub_runner() {
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
    // aqm_events.csv must be a valid CSV for NDJSON/TLA export when TLC baseline is enabled.
    fs::write(log_path.join("aqm_events.csv"), "time_ns,event_id\n1,0\n")
        .expect("write aqm_events.csv");

    // dcqcn_events.csv must be a valid CSV for NDJSON export (time_ns/event_id required).
    fs::write(
        log_path.join("dcqcn_events.csv"),
        "time_ns,event_id,kind,endpoint_id,flow_id\n1,0,timer_tick,0,0\n",
    )
    .expect("write dcqcn_events.csv");

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

    // Create a stub TLC runner that reports a full-trace depth (Len(trace)+1).
    let tlc_stub = tmp.path().join("tlc_stub.sh");
    let mut f = fs::File::create(&tlc_stub).expect("create tlc stub");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(f, "echo The depth of the complete state graph search is 2.").unwrap();
    writeln!(f, "exit 0").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&tlc_stub).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tlc_stub, perms).unwrap();
    }

    let tla_dir = format!("{}/tla", env!("CARGO_MANIFEST_DIR"));

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
        "--tlc-check",
        "--tlc-bin",
        tlc_stub.to_str().unwrap(),
        "--tlc-spec-dir",
        &tla_dir,
    ]);

    let output = cmd.output().expect("run leanguard-run");
    assert!(output.status.success(), "leanguard-run exit code");

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse leanguard-run JSON");

    assert_eq!(v["accept"].as_bool(), Some(true));
    assert_eq!(v["tlc_accept"].as_bool(), Some(true));
    assert!(v["tlc_results"].is_array(), "expected tlc_results array");
    assert!(
        v["tlc_results"][0]["status"].as_str() == Some("accept"),
        "expected tlc_results[0].status=accept"
    );
}

#[test]
fn leanguard_run_tlc_reject_reports_first_failure_by_diameter_prefix() {
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

    // dcqcn_events.csv must be a valid CSV for NDJSON export (time_ns/event_id required).
    fs::write(
        log_path.join("dcqcn_events.csv"),
        "time_ns,event_id,kind,endpoint_id,flow_id\n1,0,timer_tick,0,0\n",
    )
    .expect("write dcqcn_events.csv");

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

    // Create a stub TLC runner that reports an empty matched prefix (Diameter 1 => prefix 0).
    let tlc_stub = tmp.path().join("tlc_stub_fail.sh");
    let mut f = fs::File::create(&tlc_stub).expect("create tlc stub");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(f, "echo Diameter: 1").unwrap();
    writeln!(f, "exit 0").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&tlc_stub).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tlc_stub, perms).unwrap();
    }

    let tla_dir = format!("{}/tla", env!("CARGO_MANIFEST_DIR"));

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
        "--tlc-check",
        "--tlc-bin",
        tlc_stub.to_str().unwrap(),
        "--tlc-spec-dir",
        &tla_dir,
    ]);

    let output = cmd.output().expect("run leanguard-run");
    assert!(output.status.success(), "leanguard-run exit code");

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse leanguard-run JSON");

    assert_eq!(v["accept"].as_bool(), Some(true));
    assert_eq!(v["tlc_accept"].as_bool(), Some(false));

    // Pick the DCQCN TLC result (avoid depending on the ordering of `tlc_results`).
    let tlc_results = v["tlc_results"]
        .as_array()
        .expect("expected tlc_results array");
    let dcqcn = tlc_results
        .iter()
        .find(|r| {
            r["module"]
                .as_str()
                .is_some_and(|m| m.ends_with("DcqcnTrace.tla"))
        })
        .expect("missing DcqcnTrace TLC result");

    let failure = &dcqcn["first_failure"];
    assert_eq!(failure["index"].as_u64(), Some(1));
    assert_eq!(failure["time_ns"].as_u64(), Some(1));
    assert_eq!(failure["event_id"].as_u64(), Some(0));
    assert_eq!(failure["kind"].as_str(), Some("timer_tick"));
}

#[test]
fn leanguard_run_tlc_rejects_on_invariant_violation_exit_code() {
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
        r#"{"version":1,"traces":["dcqcn_events.csv"]}"#,
    )
    .expect("write manifest");

    // dcqcn_events.csv must be a valid CSV for NDJSON export (time_ns/event_id required).
    fs::write(
        log_path.join("dcqcn_events.csv"),
        "time_ns,event_id,kind,endpoint_id,flow_id\n1,0,timer_tick,0,0\n",
    )
    .expect("write dcqcn_events.csv");

    // Create stub checker executables.
    for exe in ["dcqcn_check"] {
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

    // Create a stub TLC runner that signals an invariant violation via non-zero exit.
    let tlc_stub = tmp.path().join("tlc_stub_invariant_fail.sh");
    let mut f = fs::File::create(&tlc_stub).expect("create tlc stub");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(f, "echo Error: Invariant ProgressOk is violated.").unwrap();
    writeln!(f, "echo The depth of the complete state graph search is 1.").unwrap();
    writeln!(f, "exit 1").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&tlc_stub).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tlc_stub, perms).unwrap();
    }

    let tla_dir = format!("{}/tla", env!("CARGO_MANIFEST_DIR"));

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
        "--tlc-check",
        "--tlc-bin",
        tlc_stub.to_str().unwrap(),
        "--tlc-spec-dir",
        &tla_dir,
    ]);

    let output = cmd.output().expect("run leanguard-run");
    assert!(output.status.success(), "leanguard-run exit code");

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse leanguard-run JSON");

    assert_eq!(v["accept"].as_bool(), Some(true));
    assert_eq!(v["tlc_accept"].as_bool(), Some(false));

    let tlc_results = v["tlc_results"]
        .as_array()
        .expect("expected tlc_results array");
    let dcqcn = tlc_results
        .iter()
        .find(|r| {
            r["module"]
                .as_str()
                .is_some_and(|m| m.ends_with("DcqcnTrace.tla"))
        })
        .expect("missing DcqcnTrace TLC result");

    assert_eq!(dcqcn["status"].as_str(), Some("reject"));
    let failure = &dcqcn["first_failure"];
    assert_eq!(failure["index"].as_u64(), Some(1));
    assert_eq!(failure["time_ns"].as_u64(), Some(1));
    assert_eq!(failure["event_id"].as_u64(), Some(0));
    assert_eq!(failure["kind"].as_str(), Some("timer_tick"));
}

#[test]
fn leanguard_run_tlc_accept_parses_tool_wrapped_depth() {
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

    fs::write(
        log_path.join("traces.json"),
        r#"{"version":1,"traces":["dcqcn_events.csv"]}"#,
    )
    .expect("write manifest");

    fs::write(
        log_path.join("dcqcn_events.csv"),
        "time_ns,event_id,kind,endpoint_id,flow_id\n1,0,timer_tick,0,0\n",
    )
    .expect("write dcqcn_events.csv");

    // Create stub checker executable.
    let checker_path = checker_dir.join("dcqcn_check");
    let mut f = fs::File::create(&checker_path).expect("create stub checker");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(f, "echo ACCEPT").unwrap();
    writeln!(f, "exit 0").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&checker_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&checker_path, perms).unwrap();
    }

    // Simulate TLC -tool output wrapping the depth line.
    let tlc_stub = tmp.path().join("tlc_stub_tool_mode.sh");
    let mut f = fs::File::create(&tlc_stub).expect("create tlc stub");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(f, "echo @\\!@\\!@\\!STARTMSG 9999:0 @\\!@\\!@\\!The depth of the complete state graph search is 2.@\\!@\\!@\\!ENDMSG 9999").unwrap();
    writeln!(f, "exit 0").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&tlc_stub).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tlc_stub, perms).unwrap();
    }

    let tla_dir = format!("{}/tla", env!("CARGO_MANIFEST_DIR"));

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
        "--tlc-check",
        "--tlc-bin",
        tlc_stub.to_str().unwrap(),
        "--tlc-spec-dir",
        &tla_dir,
    ]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"tlc_accept\": true"));
}

#[test]
fn leanguard_run_tlc_rejects_with_l_fallback_when_no_diameter() {
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

    fs::write(
        log_path.join("traces.json"),
        r#"{"version":1,"traces":["dcqcn_events.csv"]}"#,
    )
    .expect("write manifest");

    fs::write(
        log_path.join("dcqcn_events.csv"),
        "time_ns,event_id,kind,endpoint_id,flow_id\n1,0,timer_tick,0,0\n",
    )
    .expect("write dcqcn_events.csv");

    // Create stub checker executable.
    let checker_path = checker_dir.join("dcqcn_check");
    let mut f = fs::File::create(&checker_path).expect("create stub checker");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(f, "echo ACCEPT").unwrap();
    writeln!(f, "exit 0").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&checker_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&checker_path, perms).unwrap();
    }

    // Invariant violation by the initial state: no depth/diameter line, but `l = 1`.
    let tlc_stub = tmp.path().join("tlc_stub_initial_invariant_fail.sh");
    let mut f = fs::File::create(&tlc_stub).expect("create tlc stub");
    writeln!(f, "#!/bin/sh").unwrap();
    writeln!(
        f,
        "echo Error: Invariant ProgressOk is violated by the initial state:"
    )
    .unwrap();
    writeln!(f, "echo l = 1").unwrap();
    writeln!(f, "exit 1").unwrap();
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&tlc_stub).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tlc_stub, perms).unwrap();
    }

    let tla_dir = format!("{}/tla", env!("CARGO_MANIFEST_DIR"));

    let mut cmd = cargo_bin_cmd!("leanguard-run");
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--mode",
        "check-only",
        "--checker-dir",
        checker_dir.to_str().unwrap(),
        "--tlc-check",
        "--tlc-bin",
        tlc_stub.to_str().unwrap(),
        "--tlc-spec-dir",
        &tla_dir,
    ]);

    let output = cmd.output().expect("run leanguard-run");
    assert!(output.status.success(), "leanguard-run exit code");

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse leanguard-run JSON");

    assert_eq!(v["accept"].as_bool(), Some(true));
    assert_eq!(v["tlc_accept"].as_bool(), Some(false));

    let tlc_results = v["tlc_results"]
        .as_array()
        .expect("expected tlc_results array");
    let dcqcn = tlc_results
        .iter()
        .find(|r| {
            r["module"]
                .as_str()
                .is_some_and(|m| m.ends_with("DcqcnTrace.tla"))
        })
        .expect("missing DcqcnTrace TLC result");

    assert_eq!(dcqcn["status"].as_str(), Some("reject"));
    let failure = &dcqcn["first_failure"];
    assert_eq!(failure["index"].as_u64(), Some(1));
    assert_eq!(failure["time_ns"].as_u64(), Some(1));
    assert_eq!(failure["event_id"].as_u64(), Some(0));
    assert_eq!(failure["kind"].as_str(), Some("timer_tick"));
}
