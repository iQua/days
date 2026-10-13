//! P18 diet-host, design option 3(b) (the user's ruling): readback releases the device plan
//! before it decodes.
//!
//! Readback decoded the run's result while every device plane was still allocated: at the
//! 1,024-GPU flagship the joint peak was readback's, 24.2 GB of device plan beside a 13.2 GB host
//! transient. It now copies out the words the decode reads and releases each plane as soon as its
//! words are on the host, so the decode runs with the device plan freed.
//!
//! **What is measured.** The executor's phase marks (`planner-test-hooks`) report every device
//! allocation (`device_alloc`) and release (`device_free`, from the buffers' `Drop`). The plan's
//! device bytes are what the run allocated up to the end of upload; the bytes released between
//! the end of the run and the end of readback must cover them: the whole plan is gone before the
//! result is returned, rather than after.
//!
//! Run: `cargo test --release -p days --features test,metal --test p18_readback_releases_device`
//! on the Mac, `--features test,cuda` on a CUDA host.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]

use std::cell::Cell;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{ObservationMode, SimulationImage};

thread_local! {
    static STAGE: Cell<u8> = const { Cell::new(0) };
    static PLAN_BYTES: Cell<usize> = const { Cell::new(0) };
    static RELEASED_IN_READBACK: Cell<usize> = const { Cell::new(0) };
}

/// Stages: 0 before validation, 1 planning and upload, 2 the run, 3 readback, 4 after.
fn phase(name: &'static str, bytes: usize) {
    match (name, STAGE.get()) {
        ("validated", _) => STAGE.set(1),
        ("device_alloc", 1) => PLAN_BYTES.set(PLAN_BYTES.get() + bytes),
        ("uploaded", _) => STAGE.set(2),
        ("executed", _) => STAGE.set(3),
        ("device_free", 3) => RELEASED_IN_READBACK.set(RELEASED_IN_READBACK.get() + bytes),
        ("read_back", _) => STAGE.set(4),
        _ => {}
    }
}

fn b4() -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/b4-simai.toml");
    compile_config(&path).expect("b4 lowers")
}

fn check(backend: &str, run: impl FnOnce()) {
    days_executor::phase_marks::set_phase_hook_for_testing(Some(phase));
    run();
    days_executor::phase_marks::set_phase_hook_for_testing(None);
    let (plan, released) = (PLAN_BYTES.get(), RELEASED_IN_READBACK.get());
    println!(
        "record=p18_readback_releases_device backend={backend} fixture=b4 plan_device_bytes={plan} \
         released_during_readback={released}"
    );
    assert!(plan > 0, "{backend}: the run uploaded a plan");
    assert!(
        released >= plan,
        "{backend}: readback released {released} B of a {plan} B device plan before returning: \
         it decodes beside the plan"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn metal_readback_releases_the_device_plan() {
    use days_executor::{MetalConfig, MetalExecutor};
    let image = b4();
    let executor = MetalExecutor::new().expect("Metal executor");
    check("metal", || {
        executor
            .run_with_observations(
                &image,
                Some(29_000_000),
                MetalConfig {
                    capacity_caps: days::STOCK_CAPACITY_CAPS,
                    ..MetalConfig::default()
                },
                ObservationMode::Summary,
            )
            .expect("Metal runs b4");
    });
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_readback_releases_the_device_plan() {
    use days_executor::{CudaConfig, CudaExecutor};
    let image = b4();
    let executor = CudaExecutor::new().expect("CUDA executor");
    check("cuda", || {
        executor
            .run_with_observations(
                &image,
                Some(29_000_000),
                CudaConfig {
                    capacity_caps: days::STOCK_CAPACITY_CAPS,
                    ..CudaConfig::default()
                },
                ObservationMode::Summary,
            )
            .expect("CUDA runs b4");
    });
}
