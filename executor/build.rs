use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

fn main() {
    println!("cargo:rerun-if-env-changed=NVCC");
    println!("cargo:rerun-if-changed=src/cuda_kernels.cu");
    println!("cargo:rerun-if-changed=src/cuda_round_body.inc");

    if env::var_os("CARGO_FEATURE_CUDA").is_none() {
        return;
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo provides OUT_DIR"));
    // P14 round 3: one complete module per round-kernel build, from one source. Each carries every
    // shared kernel and exactly one round kernel, so a run executes a module laid out like `main`'s
    // (plain) or the P14 control's (mechanisms).
    let modules = [
        ("0", out_dir.join("days_cuda_kernels.fatbin")),
        ("1", out_dir.join("days_cuda_kernels_mechanisms.fatbin")),
    ];
    if env::var_os("CARGO_FEATURE_CUDA_PLANNER_TEST").is_some()
        && env::var_os("CARGO_FEATURE_CUDA_TEST_HOOKS").is_none()
    {
        // The planner equality surface constructs host plans only. Keep it runnable on hosts
        // without nvcc while ensuring full CUDA test-hook and all-feature builds compile kernels.
        for (_, output) in &modules {
            fs::write(output, []).expect("host-only CUDA planner placeholder must be written");
        }
        return;
    }

    let nvcc = env::var_os("NVCC").unwrap_or_else(|| OsString::from("nvcc"));
    let version = Command::new(&nvcc)
        .arg("--version")
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "the `cuda` feature requires nvcc, but `{}` could not be executed: {error}. \
             Install the CUDA toolkit and put nvcc on PATH (or set NVCC), or build without \
             `--features cuda`",
                nvcc.to_string_lossy()
            )
        });
    require_success(&nvcc, "version check", version);

    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("Cargo provides CARGO_MANIFEST_DIR"),
    );
    let source = manifest_dir.join("src/cuda_kernels.cu");
    // P16 H4: test-hook builds count the device RESUME scans' work (`DAYS_RESUME_SCAN_COUNT`);
    // P17 merge: they also count and audit the exchange merge (`DAYS_MERGE_AUDIT`). Production
    // builds compile the kernels without either.
    let hook_defines: &[&str] = if env::var_os("CARGO_FEATURE_CUDA_TEST_HOOKS").is_some() {
        &["-DDAYS_RESUME_SCAN_COUNT=1", "-DDAYS_MERGE_AUDIT=1"]
    } else {
        &[]
    };
    // The two compiles are independent: run them concurrently and wait for both.
    let compiling = modules.map(|(module, output)| {
        let child = Command::new(&nvcc)
            .arg("-std=c++17")
            .arg("-O3")
            .arg("-fatbin")
            .arg("-lineinfo")
            .arg("--diag-suppress=177")
            .arg(format!("-DDAYS_ROUND_MODULE={module}"))
            .args(hook_defines)
            .arg("--generate-code=arch=compute_121,code=sm_121")
            .arg("--generate-code=arch=compute_89,code=sm_89")
            .arg("--generate-code=arch=compute_86,code=sm_86")
            .arg("-o")
            .arg(&output)
            .arg(&source)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| {
                panic!(
                    "the `cuda` feature found nvcc at `{}`, but failed to execute it: {error}",
                    nvcc.to_string_lossy()
                )
            });
        (module, child)
    });
    for (module, child) in compiling {
        let compiled = child.wait_with_output().unwrap_or_else(|error| {
            panic!("nvcc for round module {module} could not be awaited: {error}",)
        });
        require_success(
            &nvcc,
            &format!("sm_121 + sm_89 + sm_86 fatbin compilation of round module {module}"),
            compiled,
        );
    }
}

fn require_success(nvcc: &OsString, operation: &str, output: Output) {
    if output.status.success() {
        return;
    }
    panic!(
        "the `cuda` feature requires a CUDA 13 nvcc capable of compiling sm_121, sm_89 and sm_86; \
         `{}` failed during {operation} with status {}.\nstdout:\n{}\nstderr:\n{}",
        nvcc.to_string_lossy(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
