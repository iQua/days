use std::path::Path;
use std::process::Command;

use xtask::{
    AllowedDevDependency, AllowedFeatureGate, audit_boundary_metadata, audit_semantic_feature_gates,
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
        predicate: r#"all(feature = "metal-spike", target_vendor = "apple")"#,
        count: 7,
        purpose: "Metal modules and public APIs require the Apple Metal toolchain",
    },
    AllowedFeatureGate {
        path: "cuda.rs",
        predicate: r#"feature = "cuda-test-hooks""#,
        count: 19,
        purpose: "CUDA-only fault injection, capacity, and T20l readback- and plane-word-accounting hooks",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "metal-test-hooks""#,
        count: 26,
        purpose: "Metal-only panic, fault-injection, T20l readback accounting, and T21 dominant-arena occupancy readback hooks",
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
        count: 4,
        purpose: "CUDA host-plan equality hook is enabled by the standard test feature",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 4,
        purpose: "Metal host-plan equality hook is enabled by the standard test feature",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal-spike", target_vendor = "apple"))"#,
        count: 1,
        purpose: "T20l fix 2's readback-compaction sizing module compiles only for the crate's own unit tests and the two device backends that gather with it",
    },
    AllowedFeatureGate {
        path: "device_capacity.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal-spike", target_vendor = "apple"))"#,
        count: 18,
        purpose: "shared device-arena cap, per-entity retry, and T20l warm-start replay helpers compile only for tests and device backends",
    },
    AllowedFeatureGate {
        path: "tcp_ledger_ring.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal-spike", target_vendor = "apple"))"#,
        count: 3,
        purpose: "T20i ledger-ring metadata indices and occupancy readback exist only for tests and device backends; T20l fix 2 moved the readback's own ring walk onto the device, so `ledger_record_slot` narrowed to `cfg(test)` and left this group",
    },
    AllowedFeatureGate {
        path: "device_sizing.rs",
        predicate: r#"any(test, all(feature = "planner-test-hooks", feature = "cuda"), all(feature = "planner-test-hooks", feature = "metal-spike", target_vendor = "apple"))"#,
        count: 1,
        purpose: "exact production-layout reports exist only for the crate's own unit test and the two device planner probes (`cuda::size_cuda_plan_for_testing`, `metal::size_metal_plan_for_testing`), which are `planner-test-hooks` items inside `cuda` / `metal-spike` modules",
    },
    AllowedFeatureGate {
        path: "cpu.rs",
        predicate: r#"all(feature = "metal-spike", target_vendor = "apple")"#,
        count: 9,
        purpose: "CPU replay/window instrumentation consumed by the Apple Metal harness",
    },
    AllowedFeatureGate {
        path: "safe_horizon.rs",
        predicate: r#"all(feature = "metal-spike", target_vendor = "apple")"#,
        count: 39,
        purpose: "replay and window capture instrumentation for Apple Metal profiling",
    },
    AllowedFeatureGate {
        path: "scalar.rs",
        predicate: r#"all(feature = "metal-spike", target_vendor = "apple")"#,
        count: 1,
        purpose: "queue occupancy observation used by Apple Metal replay capture",
    },
    AllowedFeatureGate {
        path: "device_scheduler.rs",
        predicate: r#"not(any(feature = "cuda", all(feature = "metal-spike", target_vendor = "apple")))"#,
        count: 1,
        purpose: "suppress dead-code warnings when neither device toolchain backend is built",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"any(feature = "cuda", all(feature = "metal-spike", target_vendor = "apple"))"#,
        count: 1,
        purpose: "shared planner lookup tables exist only when a device planner is built",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal-test-hooks", target_vendor = "apple")"#,
        count: 1,
        purpose: "Metal capacity, plane-word, and dominant-arena test hooks require dedicated test tools and Apple Metal",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "cuda", feature = "planner-test-hooks")"#,
        count: 2,
        purpose: "CUDA full-plan equality hook requires the backend and standard test helpers",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal-spike", feature = "planner-test-hooks", target_vendor = "apple")"#,
        count: 2,
        purpose: "Metal full-plan equality hook requires standard test helpers and Apple Metal",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"any(feature = "cuda", all(feature = "metal-spike", target_vendor = "apple"))"#,
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
        count: 19,
        purpose: "legacy quadratic helpers exist only for unit and standard full-plan equality tests. T21 added two: the retained `planning_horizon_ns` field and its literal, which is an INPUT the legacy ledger-bound arm recomputes from and the precomputed table has already baked in, so a production build must not carry it",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"all(feature = "planner-test-hooks", any(feature = "cuda", all(feature = "metal-spike", target_vendor = "apple")))"#,
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
        count: 2,
        purpose: "pre-index validator scans and their equality hook exist only for standard tests",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 1,
        purpose: "validator flow-index equality hook is exported only for standard tests",
    },
];

const T13F_FULL_LOAD_TESTS: &[&str] = &[
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
        "t13f-full-load" => run_t13f_full_load(workspace),
        _ => {
            print_usage();
            std::process::exit(2);
        }
    }
}

fn print_usage() {
    eprintln!("usage: cargo xtask <audit|t13f-full-load>");
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

    if failures.is_empty() {
        println!("legacy boundary audit: PASS");
        println!("semantic feature-gate audit: PASS");
    } else {
        eprintln!("{}", failures.join("\n\n"));
        std::process::exit(1);
    }
}

fn run_t13f_full_load(workspace: &Path) {
    let status = Command::new("cargo")
        .args([
            "nextest",
            "run",
            "--package",
            "days-validation",
            "--features",
            "metal-spike",
            "--test",
            "t13f_width_via_load_full",
            "--test-threads",
            "5",
        ])
        .args(T13F_FULL_LOAD_TESTS)
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
