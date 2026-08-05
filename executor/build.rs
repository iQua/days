use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::{Command, Output};

fn main() {
    println!("cargo:rerun-if-env-changed=NVCC");
    println!("cargo:rerun-if-changed=src/cuda_kernels.cu");

    if env::var_os("CARGO_FEATURE_CUDA").is_none() {
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
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo provides OUT_DIR"))
        .join("days_cuda_kernels.fatbin");
    let compiled = Command::new(&nvcc)
        .arg("-std=c++17")
        .arg("-O3")
        .arg("-fatbin")
        .arg("-lineinfo")
        .arg("--diag-suppress=177")
        .arg("--generate-code=arch=compute_121,code=sm_121")
        .arg("--generate-code=arch=compute_89,code=sm_89")
        .arg("-o")
        .arg(&output)
        .arg(&source)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "the `cuda` feature found nvcc at `{}`, but failed to execute it: {error}",
                nvcc.to_string_lossy()
            )
        });
    require_success(&nvcc, "sm_121 + sm_89 fatbin compilation", compiled);
}

fn require_success(nvcc: &OsString, operation: &str, output: Output) {
    if output.status.success() {
        return;
    }
    panic!(
        "the `cuda` feature requires a CUDA 13 nvcc capable of compiling sm_121 and sm_89; \
         `{}` failed during {operation} with status {}.\nstdout:\n{}\nstderr:\n{}",
        nvcc.to_string_lossy(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
