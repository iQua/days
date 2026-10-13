//! P18 diet-host c4: a device run's image is validated once (ruling 2(a)).
//!
//! The `days` CLI lowered an image, validated it for Scalar inside `compile`, and the device entry
//! validated it again for the device: 431 G instructions at the 1,024-GPU flagship (P16 aicb-2b
//! review). `compile_config_validated` now validates once for the target device (the Scalar
//! passes and the device's own refusals, `ValidatedImage`), and a device entry given that
//! `ValidatedImage` does not validate again. A borrowed image is still validated at the entry.
//!
//! **What is measured.** Each run of the validation pipeline marks `validation_pipeline` through
//! the executor's phase hook (`planner-test-hooks`), counted here on the test's thread.
//!
//! Run: `cargo test --release -p days --features test,metal --test p18_validate_once` on the
//! Mac, `--features test,cuda` on a CUDA host.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]

use std::cell::Cell;
use std::path::PathBuf;

use days::scenario::{compile_config, compile_config_validated};
use days::topos::route::RouteWorkers;
use days_executor::{Backend, ObservationMode, RunImage};

thread_local! {
    static VALIDATIONS: Cell<usize> = const { Cell::new(0) };
}

fn count(phase: &'static str, _: usize) {
    if phase == "validation_pipeline" {
        VALIDATIONS.set(VALIDATIONS.get() + 1);
    }
}

fn b4() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/b4-simai.toml")
}

/// Validation pipelines run by lowering b4 for `backend` and running it with `run`, the CLI's
/// path (`compile_config_validated` and the validated image), and the borrowed path
/// (`compile_config` and `&image`).
fn validations(backend: Backend, run: impl Fn(RunImage<'_>)) -> (usize, usize) {
    days_executor::phase_marks::set_phase_hook_for_testing(Some(count));
    VALIDATIONS.set(0);
    let (image, _) =
        compile_config_validated(b4(), RouteWorkers::available(), backend).expect("b4 lowers");
    run(image.into());
    let validated = VALIDATIONS.take();
    let image = compile_config(b4()).expect("b4 lowers");
    run((&image).into());
    let borrowed = VALIDATIONS.take();
    days_executor::phase_marks::set_phase_hook_for_testing(None);
    (validated, borrowed)
}

fn check(backend: &str, (validated, borrowed): (usize, usize)) {
    println!(
        "record=p18_validate_once backend={backend} fixture=b4 cli_path_validations={validated} \
         borrowed_path_validations={borrowed}"
    );
    assert_eq!(validated, 1, "{backend}: the CLI path validates once");
    assert_eq!(
        borrowed, 2,
        "{backend}: a borrowed image is validated at lowering and entry"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn a_metal_run_of_a_validated_image_validates_once() {
    use days_executor::{MetalConfig, MetalExecutor};
    let executor = MetalExecutor::new().expect("Metal executor");
    check(
        "metal",
        validations(Backend::Metal, |image| {
            executor
                .run_with_observations(
                    image,
                    Some(19_000_000),
                    MetalConfig::default(),
                    ObservationMode::Summary,
                )
                .expect("Metal runs b4");
        }),
    );
}

#[cfg(feature = "cuda")]
#[test]
fn a_cuda_run_of_a_validated_image_validates_once() {
    use days_executor::{CudaConfig, CudaExecutor};
    let executor = CudaExecutor::new().expect("CUDA executor");
    check(
        "cuda",
        validations(Backend::Cuda, |image| {
            executor
                .run_with_observations(
                    image,
                    Some(19_000_000),
                    CudaConfig::default(),
                    ObservationMode::Summary,
                )
                .expect("CUDA runs b4");
        }),
    );
}
