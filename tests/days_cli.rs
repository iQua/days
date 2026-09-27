//! End-to-end checks of the `days` binary: every host engine prints its records, the engines
//! agree on the complete-state fingerprint, and misuse is refused with an explicit error.

use assert_cmd::cargo::cargo_bin_cmd;

/// The TCP smoke fixture's complete-state fingerprint, identical on Scalar, CPU, Metal and CUDA.
const SMOKE: &str = "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml";
const SMOKE_RESULT: &str = "result_bytes=101690 result_fnv1a64=d7de73ee33579682";

fn days(arguments: &[&str]) -> (bool, String, String) {
    let output = cargo_bin_cmd!("days")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(arguments)
        .output()
        .expect("days must launch");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout must be UTF-8"),
        String::from_utf8(output.stderr).expect("stderr must be UTF-8"),
    )
}

fn record<'a>(stdout: &'a str, name: &str) -> Vec<&'a str> {
    let prefix = format!("record={name} ");
    stdout
        .lines()
        .filter(|line| line.starts_with(&prefix))
        .collect()
}

#[test]
fn scalar_prints_its_counters_and_the_fingerprint() {
    let (ok, stdout, stderr) = days(&["configs/ci/executor_smoke.toml", "--engine", "scalar"]);
    assert!(ok, "{stderr}");
    let scalar = record(&stdout, "days_scalar");
    assert_eq!(scalar.len(), 1, "{stdout}");
    assert!(scalar[0].starts_with(
        "record=days_scalar config=configs/ci/executor_smoke.toml stop_time_ns=10000000 \
         next_pending_ns=None pending_events=0 certified_lookahead_ns=Some(512000) events=160 \
         sourced_packets=16 sourced_bytes=8192 received_packets=16 received_bytes=8192 \
         dropped_packets=0 dropped_bytes=0 wall_ns="
    ));
    assert_eq!(record(&stdout, "days_result").len(), 1, "{stdout}");
    assert!(record(&stdout, "days_protocol").is_empty());
}

#[test]
fn scalar_and_cpu_agree_on_the_complete_state_fingerprint() {
    let (ok, stdout, stderr) = days(&[SMOKE, "--engine", "scalar"]);
    assert!(ok, "{stderr}");
    let scalar = record(&stdout, "days_result");
    assert_eq!(scalar.len(), 1);
    assert!(scalar[0].starts_with("record=days_result engine=scalar "));
    assert!(scalar[0].contains(SMOKE_RESULT), "{}", scalar[0]);

    for workers in ["1", "2"] {
        let (ok, stdout, stderr) = days(&[
            SMOKE,
            "--engine",
            "cpu",
            "--workers",
            workers,
            "--repetitions",
            "2",
        ]);
        assert!(ok, "{stderr}");
        let cpu = record(&stdout, "days_cpu");
        assert_eq!(cpu.len(), 2, "one record per repetition:\n{stdout}");
        for (repetition, line) in cpu.iter().enumerate() {
            assert!(line.starts_with(&format!(
                "record=days_cpu config={SMOKE} repetition={repetition} mode=cpu \
                 workers={workers} chunk=static static_partition=route-load \
                 straggler_threshold=none rounds=64 events=8464 "
            )));
        }
        let results = record(&stdout, "days_result");
        assert_eq!(results.len(), 2);
        for line in results {
            assert!(line.starts_with("record=days_result engine=cpu "));
            assert!(line.contains(SMOKE_RESULT), "{line}");
        }
    }
}

#[test]
fn misuse_is_refused_with_an_explicit_error() {
    let (ok, _, stderr) = days(&[SMOKE, "--engine", "scalar", "--workers", "2"]);
    assert!(!ok);
    assert!(stderr.contains("--workers"), "{stderr}");

    let (ok, _, stderr) = days(&[SMOKE, "--engine", "cpu"]);
    assert!(!ok);
    assert!(stderr.contains("--workers"), "{stderr}");

    let (ok, _, stderr) = days(&[SMOKE, "--engine", "cpu", "--workers", "0"]);
    assert!(!ok);
    assert!(stderr.contains("--workers"), "{stderr}");

    let (ok, _, _) = days(&[SMOKE, "--engine", "device"]);
    assert!(!ok, "the retired `device` engine must not parse");
}

fn days_exit_code(arguments: &[&str]) -> Option<i32> {
    cargo_bin_cmd!("days")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(arguments)
        .output()
        .expect("days must launch")
        .status
        .code()
}

/// The exit-code contract: 2 for every argument or availability error (clap's own code for
/// argument errors), 1 for a failure while lowering or running, 0 for success.
#[test]
fn exit_codes_separate_argument_errors_from_run_failures() {
    let argument_errors: &[&[&str]] = &[
        &[SMOKE, "--engine", "bogus"],
        &[SMOKE],
        &[SMOKE, "--engine", "cpu", "--workers", "1", "--chunk", "big"],
        &[SMOKE, "--engine", "cpu"],
        &[SMOKE, "--engine", "cpu", "--workers", "0"],
        &[
            SMOKE,
            "--engine",
            "cpu",
            "--workers",
            "1",
            "--repetitions",
            "0",
        ],
        &[SMOKE, "--engine", "scalar", "--workers", "2"],
        &[
            SMOKE,
            "--engine",
            "scalar",
            "--round-threads-per-block",
            "64",
        ],
        &[
            SMOKE,
            "--engine",
            "cpu",
            "--workers",
            "1",
            "--max-capacity-retries",
            "0",
        ],
        &[SMOKE, "--engine", "metal", "--repetitions", "2"],
        &[SMOKE, "--engine", "cuda", "--workers", "2"],
    ];
    for arguments in argument_errors {
        assert_eq!(
            days_exit_code(arguments),
            Some(2),
            "argument error: {arguments:?}"
        );
    }
    if !cfg!(all(feature = "metal", target_vendor = "apple")) {
        assert_eq!(days_exit_code(&[SMOKE, "--engine", "metal"]), Some(2));
    }
    if !cfg!(feature = "cuda") {
        assert_eq!(days_exit_code(&[SMOKE, "--engine", "cuda"]), Some(2));
    }

    assert_eq!(
        days_exit_code(&["configs/does_not_exist.toml", "--engine", "scalar"]),
        Some(1),
        "a missing fixture is a run-time failure"
    );
    assert_eq!(
        days_exit_code(&[
            SMOKE,
            "--engine",
            "cpu",
            "--workers",
            "2",
            "--dedicated",
            "0",
            "--straggler-threshold",
            "5",
        ]),
        Some(1),
        "a configuration the executor refuses at run time is a run-time failure"
    );
    assert_eq!(
        days_exit_code(&["configs/ci/executor_smoke.toml", "--engine", "scalar"]),
        Some(0)
    );
}

#[test]
fn an_unbuilt_device_engine_is_an_error_that_names_its_feature() {
    if !cfg!(all(feature = "metal", target_vendor = "apple")) {
        let (ok, stdout, stderr) = days(&[SMOKE, "--engine", "metal"]);
        assert!(!ok);
        assert!(stdout.is_empty(), "nothing may run: {stdout}");
        assert!(stderr.contains("--features metal"), "{stderr}");
    }
    if !cfg!(feature = "cuda") {
        let (ok, stdout, stderr) = days(&[SMOKE, "--engine", "cuda"]);
        assert!(!ok);
        assert!(stdout.is_empty(), "nothing may run: {stdout}");
        assert!(stderr.contains("--features cuda"), "{stderr}");
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn metal_matches_the_scalar_fingerprint() {
    let (ok, stdout, stderr) = days(&[SMOKE, "--engine", "metal"]);
    assert!(ok, "{stderr}");
    let protocol = record(&stdout, "days_protocol");
    assert_eq!(protocol.len(), 1, "{stdout}");
    assert!(
        protocol[0].contains(" engine=metal "),
        "every record names the engine in lowercase: {}",
        protocol[0]
    );
    assert!(
        protocol[0].ends_with(" cuda_device=none"),
        "the Metal protocol record names no CUDA device: {}",
        protocol[0]
    );
    assert_eq!(record(&stdout, "days_device").len(), 1, "{stdout}");
    let result = record(&stdout, "days_result");
    assert_eq!(result.len(), 1);
    assert!(result[0].starts_with("record=days_result engine=metal "));
    assert!(result[0].contains(SMOKE_RESULT), "{}", result[0]);
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_matches_the_scalar_fingerprint() {
    let (ok, stdout, stderr) = days(&[SMOKE, "--engine", "cuda"]);
    assert!(ok, "{stderr}");
    let protocol = record(&stdout, "days_protocol");
    assert_eq!(protocol.len(), 1, "{stdout}");
    assert!(
        protocol[0].contains(" engine=cuda "),
        "every record names the engine in lowercase: {}",
        protocol[0]
    );
    assert!(
        protocol[0].ends_with(" cuda_device=0"),
        "the protocol record names the default CUDA device: {}",
        protocol[0]
    );
    assert_eq!(record(&stdout, "days_device").len(), 1, "{stdout}");
    let result = record(&stdout, "days_result");
    assert_eq!(result.len(), 1);
    assert!(result[0].starts_with("record=days_result engine=cuda "));
    assert!(result[0].contains(SMOKE_RESULT), "{}", result[0]);
}
