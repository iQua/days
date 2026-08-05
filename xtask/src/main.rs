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
        count: 12,
        purpose: "CUDA-only fault injection, capacity, and planner measurement hooks",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "metal-test-hooks""#,
        count: 6,
        purpose: "Metal-only panic, fault-injection, and planner measurement hooks",
    },
    AllowedFeatureGate {
        path: "cuda.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 1,
        purpose: "CUDA host-plan equality hook is enabled by the standard test feature",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "planner-test-hooks""#,
        count: 1,
        purpose: "Metal host-plan equality hook is enabled by the standard test feature",
    },
    AllowedFeatureGate {
        path: "device_capacity.rs",
        predicate: r#"any(test, feature = "cuda", all(feature = "metal-spike", target_vendor = "apple"))"#,
        count: 3,
        purpose: "shared device-arena cap and retry helpers compile only for tests and device backends",
    },
    AllowedFeatureGate {
        path: "device_sizing.rs",
        predicate: r#"any(test, feature = "cuda-test-hooks", all(feature = "metal-test-hooks", target_vendor = "apple"))"#,
        count: 1,
        purpose: "exact production-layout reports exist only for tests and dedicated backend probes",
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
        predicate: r#"feature = "cuda-test-hooks""#,
        count: 1,
        purpose: "CUDA planner measurement hook is exported only for dedicated test tools",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal-test-hooks", target_vendor = "apple")"#,
        count: 1,
        purpose: "Metal planner measurement hook requires dedicated test tools and Apple Metal",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "cuda", feature = "planner-test-hooks")"#,
        count: 1,
        purpose: "CUDA full-plan equality hook requires the backend and standard test helpers",
    },
    AllowedFeatureGate {
        path: "lib.rs",
        predicate: r#"all(feature = "metal-spike", feature = "planner-test-hooks", target_vendor = "apple")"#,
        count: 1,
        purpose: "Metal full-plan equality hook requires standard test helpers and Apple Metal",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"feature = "cuda""#,
        count: 5,
        purpose: "CUDA-only MSS and TCP segment-capacity lookup behavior",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"all(feature = "metal-spike", target_vendor = "apple")"#,
        count: 5,
        purpose: "Metal-only one-byte TCP minimum behavior",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"any(test, feature = "planner-test-hooks")"#,
        count: 18,
        purpose: "legacy quadratic helpers exist only for unit and standard full-plan equality tests",
    },
    AllowedFeatureGate {
        path: "planner_capacity.rs",
        predicate: r#"any(debug_assertions, test, feature = "planner-test-hooks")"#,
        count: 1,
        purpose: "legacy minimum scan supports sampled debug checks and equality tests",
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
