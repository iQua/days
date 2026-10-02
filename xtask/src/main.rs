use std::path::Path;
use std::process::Command;

use xtask::{
    AllowedDevDependency, AllowedFeatureGate, AllowedTableScanner, audit_boundary_metadata,
    audit_scalar_table_access, audit_semantic_feature_gates, audit_table_scans,
};

const LEGACY_DEV_DEPENDENCIES: &[AllowedDevDependency] = &[AllowedDevDependency {
    package: "days-validation",
    dependency: "days-legacy",
    purpose: "test-only executor/legacy trajectory, fixture, and corpus comparisons",
}];

const BACKEND_FEATURE_GATES: &[AllowedFeatureGate] = &[
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"feature = "cuda""#,
        count: 2,
        purpose: "CUDA module and public API exist only when the CUDA toolchain backend is built",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal", target_vendor = "apple")"#,
        count: 2,
        purpose: "Metal modules and public APIs require the Apple Metal toolchain",
    },
    AllowedFeatureGate {
        path: "cuda.rs",
        predicate: r#"feature = "cuda-test-hooks""#,
        count: 38,
        purpose: "CUDA-only fault injection, capacity, and worklist-compaction hooks, the P14 round-kernel override that forces either `days_round` build (the config field, its default, and its one read), the P14 round-3 one-module-per-run probes (the launched-kernel record on the timing and the run and its two fills, the three handle-identity helpers that build it, the module-contents probe, and the mixed-array record probe), the P14 round-4 live-round-module count: its atomic import, the per-device counter and its initialisation, each loaded module's entry (the field, its registration, the entry type and its two impls), and the count at graph capture on the timing and the run with its read and two fills, and the P14 cuda-host per-thread run-step probe (its record body and its take)",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "metal-test-hooks""#,
        count: 22,
        purpose: "Metal-only panic, fault-injection, T21 occupancy, and worklist-compaction hooks, and the P14 round-kernel override that forces either `days_round` pipeline (the config field, its default, and its one read)",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"not(feature = "metal-test-hooks")"#,
        count: 1,
        purpose: "the production Metal source omits T21's test-only dominant-arena occupancy writes and physical metadata tails",
    },
    AllowedFeatureGate {
        path: "cuda.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 6,
        purpose: "CUDA host-plan equality hook, P14 Lane B's mechanism-plane word probe and the P14 cuda-host PFC-state scan probe are enabled by the standard test feature",
    },
    AllowedFeatureGate {
        path: "device_pfc.rs",
        predicate: r#"all(feature = "cuda", feature = "planner-test-hooks")"#,
        count: 3,
        purpose: "the P14 cuda-host PFC-state scan probe, compiled exactly where its CUDA planner hook is: a per-thread scan counter (empty in every other build), its increment in the two whole-fabric PFC scans, and its read-and-reset for the hook",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 5,
        purpose: "Metal host-plan equality hook and P14 Lane B's mechanism-plane word probe are enabled by the standard test feature",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 2,
        purpose: "T20l fix 2's readback-compaction sizing module and P14 Lane B's DCQCN device-row codecs (`device_mechanism`) compile only for the crate's own unit tests and the two device backends that use them",
    },
    AllowedFeatureGate {
        path: "device_capacity.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 18,
        purpose: "shared device-arena cap, per-entity retry, and T20l warm-start replay helpers compile only for tests and device backends",
    },
    AllowedFeatureGate {
        path: "device_event_record.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 5,
        purpose: "O2.12 compact event-record classes and codecs compile only for unit tests and the two device backends that store those records",
    },
    AllowedFeatureGate {
        path: "tcp_ledger.rs",
        predicate: r#"any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 1,
        purpose: "O2.12 device readback retains non-TCP orphan packet descriptors only for the two device backends that reconstruct compact records",
    },
    AllowedFeatureGate {
        path: "tcp_ledger_ring.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 3,
        purpose: "T20i ledger-ring metadata indices and occupancy readback exist only for tests and device backends; T20l fix 2 moved the readback's own ring walk onto the device, so `ledger_record_slot` narrowed to `cfg(test)` and left this group",
    },
    AllowedFeatureGate {
        path: "device_sizing.rs",
        predicate: r#"any(test, all(feature = "planner-test-hooks", feature = "cuda"), all(feature = "planner-test-hooks", feature = "metal", target_vendor = "apple"))"#,
        count: 1,
        purpose: "exact production-layout reports exist only for the crate's own unit test and the two device planner probes (`cuda::size_cuda_plan_for_testing`, `metal::size_metal_plan_for_testing`), which are `planner-test-hooks` items inside `cuda` / `metal` modules",
    },
    AllowedFeatureGate {
        path: "device_scheduler.rs",
        predicate: r#"not(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))"#,
        count: 1,
        purpose: "suppress dead-code warnings when neither device toolchain backend is built",
    },
    AllowedFeatureGate {
        path: "device_pfc.rs",
        predicate: r#"not(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))"#,
        count: 1,
        purpose: "P14 Lane B's PFC region codec packs and restores device planes; without a device backend only its sizing helpers are used, so suppress dead-code warnings",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 2,
        purpose: "shared planner lookup tables exist only when a device planner is built; P14's `RoundKernel` selection type is exported only with a device backend, the only builds with two `days_round` builds to select between",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal-test-hooks", target_vendor = "apple")"#,
        count: 1,
        purpose: "Metal capacity and dominant-arena test hooks require dedicated test tools and Apple Metal",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "cuda", feature = "planner-test-hooks")"#,
        count: 2,
        purpose: "CUDA full-plan equality hook requires the backend and standard test helpers",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal", feature = "planner-test-hooks", target_vendor = "apple")"#,
        count: 2,
        purpose: "Metal full-plan equality hook requires standard test helpers and Apple Metal",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))"#,
        count: 5,
        purpose: "both device planners use the one-byte minimum possible TCP tail segment",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"feature = "cuda""#,
        count: 4,
        purpose: "CUDA-only synthetic one-byte TCP planner regression",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"any(test, feature = "planner-test-hooks")"#,
        count: 21,
        purpose: "legacy quadratic helpers exist only for unit and standard full-plan equality tests. T21 added two: the retained `planning_horizon_ns` field and its literal, which is an INPUT the legacy ledger-bound arm recomputes from and the precomputed table has already baked in, so a production build must not carry it. P14 Lane B added the legacy arm of `dcqcn_generator`, and P15 lane R4 the legacy arm of `roce_generator`",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"all(feature = "planner-test-hooks", any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))"#,
        count: 1,
        purpose: "the precomputed-versus-legacy planner equality check has no host consumer: its only callers are `cuda::assert_cuda_planner_bit_equal_for_testing` and `metal::assert_metal_planner_bit_equal_for_testing`, so it carries no `test` arm",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"any(debug_assertions, test, feature = "planner-test-hooks")"#,
        count: 1,
        purpose: "legacy minimum scan supports sampled debug checks and equality tests",
    },
    AllowedFeatureGate {
        path: "validate.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 4,
        purpose: "pre-index validator scans, their two equality hooks and the stage-lookup probe exist only for standard tests",
    },
    AllowedFeatureGate {
        path: "scalar.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 2,
        purpose: "the stage-scan counting run and the stage-index equality hook exist only for standard tests",
    },
    AllowedFeatureGate {
        path: "stage_index.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 18,
        purpose: "the stage-scan probe's counters (empty in production builds), the probed tables' read counters and their counting in iterators, the pre-index stage scans and their per-host equality check exist only for standard tests",
    },
    AllowedFeatureGate {
        path: "stage_index.rs",
        predicate: r#"not(feature = "planner-test-hooks")"#,
        count: 4,
        purpose: "without the test hooks a counted iterator carries a zero-sized marker instead of its read counter, and a compile-time check pins the probe to zero size and the probed table, counted iterator and host slot to the size of what they wrap",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 1,
        purpose: "validator flow-index equality hook is exported only for standard tests",
    },
];

/// Functions of `executor/src/scalar.rs` on the collective and compute stage path: stage release
/// and progress, and the TCP transport and compute timer that carry a stage's bytes and time.
/// Each runs per event on a host holding up to 2(n - 1) stage generators, so each must reach the
/// host's tables through the counted stage view (`audit_scalar_table_access`).
const STAGE_PATH_FUNCTIONS: &[&str] = &[
    "complete_local_successors",
    "record_inbound_progress",
    "host_packet_arrival",
    "activate_ready_collectives",
    "activate_wrapped_stage",
    "start_compute_stage",
    "host_compute_timer",
    "push_stage_progress",
    "host_tcp_initial_send",
    "host_tcp_data_arrival",
    "host_tcp_ack_arrival",
    "host_pacing_timer",
    "prepare_tcp_attempts",
    "install_tcp_attempts",
    // P15: DCQCN and RoCE queue-pair transitions, keyed through the same counted view; a host can
    // hold many queue pairs (collective stages over RoCE follow in a later lane).
    "host_dcqcn_cnp_arrival",
    "host_dcqcn_control_timer",
    "host_roce_pacing_timer",
    "host_roce_feedback_arrival",
    "host_roce_timeout",
    // P15 host-link PFC: a RESUME restarts its class's pause-parked queue pairs, read by
    // generator position from the host's parked list through the same counted view.
    "host_pfc_remote_arrival",
];

/// The only functions of `executor/src/scalar.rs` that may scan a host's generator or TCP-receiver
/// table (`audit_scalar_table_access`, default-deny). None is on the stage path; every other
/// function reads those tables by key through the stage index.
const SCALAR_TABLE_SCANNERS: &[AllowedTableScanner] = &[
    AllowedTableScanner {
        scope: "host_remote_arrival",
        reason: "non-TCP feedback for constant and rate generators; TCP stage segments and ACKs dispatch to their own handlers first",
    },
    AllowedTableScanner {
        scope: "host_retransmission_timeout",
        reason: "runs per timeout firing and matches by timer identity, not flow; keying it needs a timer map refreshed at every active_timer store",
    },
    AllowedTableScanner {
        scope: "host_dcqcn_pacing_timer",
        reason: "DCQCN only; DCQCN flows cannot be stages (collectives lower only over TCP)",
    },
];

/// `executor/src/cpu.rs` runs the same transitions through `scalar::TransitionState`; its own
/// host-table reads are pool setup and a unit test. Default-deny keeps a CPU-only helper from
/// reintroducing a per-event scan.
const CPU_TABLE_SCANNERS: &[AllowedTableScanner] = &[
    AllowedTableScanner {
        scope: "route_offered_load",
        reason: "image-only load estimate for the static route-load partition, computed once when the worker pool is built",
    },
    AllowedTableScanner {
        scope: "route_load_estimator_uses_only_declared_routes_and_generator_rates",
        reason: "unit test that installs a fixture generator table",
    },
    AllowedTableScanner {
        scope: "queue_pair_tokens",
        reason: "once per CPU run when the image holds a timer token, at pool build: maps every RoCE queue pair's two tokens to its source LP, so token import is keyed",
    },
];

/// `executor/src/stage_index.rs` derives the index from the tables and keeps the retired scans as
/// test oracles; a lookup helper added there must be keyed like the rest.
const STAGE_INDEX_TABLE_SCANNERS: &[AllowedTableScanner] = &[
    AllowedTableScanner {
        scope: "build",
        reason: "derives a host's index once, when the transition state is constructed, not per event",
    },
    AllowedTableScanner {
        scope: "first_ready_by_scan",
        reason: "debug-build oracle of the indexed activation choice (debug_assert_eq! only)",
    },
    AllowedTableScanner {
        scope: "legacy_scans",
        reason: "the retired scans, compiled only for the test hooks, as the equality gate's oracle",
    },
    AllowedTableScanner {
        scope: "check_host_index",
        reason: "the equality gate's per-host checker, compiled only for the test hooks",
    },
];

const WIDTH_VIA_LOAD_FULL_TESTS: &[&str] = &[
    "width_via_load_full_load_10_holds_runtime_contract",
    "width_via_load_full_load_30_holds_runtime_contract",
    "width_via_load_full_load_50_holds_runtime_contract",
    "width_via_load_full_load_70_holds_runtime_contract",
    "width_via_load_full_load_90_holds_runtime_contract",
];

fn main() {
    let mut arguments = std::env::args().skip(1);
    let command = match (arguments.next(), arguments.next()) {
        (Some(command), None) => command,
        _ => {
            print_usage();
            std::process::exit(2);
        }
    };

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be a direct workspace member");

    match command.as_str() {
        "audit" => run_audits(workspace),
        "width-via-load-full" => run_width_via_load_full(workspace),
        _ => {
            print_usage();
            std::process::exit(2);
        }
    }
}

fn print_usage() {
    eprintln!("usage: cargo xtask <audit|width-via-load-full>");
}

fn run_audits(workspace: &Path) {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--all-features"])
        .current_dir(workspace)
        .output()
        .expect("failed to execute cargo metadata");
    if !output.status.success() {
        eprintln!(
            "cargo metadata failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::process::exit(1);
    }

    let metadata = String::from_utf8(output.stdout).expect("cargo metadata must be UTF-8 JSON");
    let mut failures = Vec::new();
    if let Err(error) = audit_boundary_metadata(&metadata, LEGACY_DEV_DEPENDENCIES) {
        failures.push(format!("legacy boundary audit failed:\n{error}"));
    }
    if let Err(error) =
        audit_semantic_feature_gates(&workspace.join("executor/src"), BACKEND_FEATURE_GATES)
    {
        failures.push(format!("semantic feature-gate audit failed:\n{error}"));
    }
    if let Err(error) = audit_scalar_table_access(
        &workspace.join("executor/src/scalar.rs"),
        STAGE_PATH_FUNCTIONS,
        SCALAR_TABLE_SCANNERS,
    ) {
        failures.push(format!("scalar.rs table-access audit failed:\n{error}"));
    }
    for (file, allow_list) in [
        ("executor/src/cpu.rs", CPU_TABLE_SCANNERS),
        ("executor/src/stage_index.rs", STAGE_INDEX_TABLE_SCANNERS),
    ] {
        if let Err(error) = audit_table_scans(&workspace.join(file), allow_list) {
            failures.push(format!("host-table scan audit of {file} failed:\n{error}"));
        }
    }

    if failures.is_empty() {
        println!("legacy boundary audit: PASS");
        println!("semantic feature-gate audit: PASS");
        println!("scalar.rs table-access audit (stage-path view, host-table scans): PASS");
        println!("host-table scan audit (cpu.rs, stage_index.rs): PASS");
    } else {
        eprintln!("{}", failures.join("\n\n"));
        std::process::exit(1);
    }
}

fn run_width_via_load_full(workspace: &Path) {
    let status = Command::new("cargo")
        .args([
            "nextest",
            "run",
            "--package",
            "days-validation",
            "--features",
            "metal",
            "--test",
            "width_via_load_full",
            "--test-threads",
            "5",
        ])
        .args(WIDTH_VIA_LOAD_FULL_TESTS)
        .current_dir(workspace)
        .status()
        .unwrap_or_else(|error| {
            eprintln!(
                "failed to execute cargo-nextest ({error}); install it with `cargo install cargo-nextest --locked`"
            );
            std::process::exit(1);
        });

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}
