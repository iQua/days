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
        count: 10,
        purpose: "CUDA-only fault injection and capacity test hooks; no protocol selection",
    },
    AllowedFeatureGate {
        path: "metal.rs",
        predicate: r#"feature = "metal-test-hooks""#,
        count: 4,
        purpose: "Metal-only panic and fault-injection test hooks; no protocol selection",
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
];

fn main() {
    let mut arguments = std::env::args().skip(1);
    match (arguments.next().as_deref(), arguments.next()) {
        (Some("audit"), None) => {}
        _ => {
            eprintln!("usage: cargo xtask audit");
            std::process::exit(2);
        }
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be a direct workspace member");
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
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
