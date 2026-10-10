//! P18 diet-host c5: readback moves a validated image's states instead of cloning them.
//!
//! A device run's readback cloned the image's host and switch states into the result while
//! every device plane was still allocated: 8.2 GB at the 1,024-GPU flagship, 18.6 GB at 2,048
//! (design finding F2). A run that consumes a `ValidatedImage` now moves the states into the
//! result; a borrowed image is still cloned.
//!
//! **What is measured.** The executor's phase marks split each run (`planner-test-hooks`); this
//! binary's allocator counts live heap bytes on the run's thread. The readback growth is the live
//! heap when readback ends (the result built, the device planes still allocated) less the live
//! heap when the run finished executing. The states' own size is measured by cloning them here.
//! On the borrowed path the result holds a clone; on the validated path it holds the moved
//! states: the borrowed growth exceeds the validated growth by at least the states' size. On CUDA
//! (sim), MoE smoke to 5 ms: states 246,192,128 B; borrowed growth 270,601,240 B both before and
//! after c5 on the validated path, 21,767,192 B after. (Whether
//! the clone raised the readback *peak* depends on the image: on the MoE smoke the peak is the
//! gathered arenas, 644 MB above the run's live heap either way; at the flagship the clone,
//! 8.2 GB, is part of it.)
//!
//! Run: `cargo test --release -p days --features test,metal --test p18_readback_memory` on the
//! Mac, `--features test,cuda` on a CUDA host.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::PathBuf;

use days::scenario::{compile_config, compile_config_validated};
use days::topos::route::RouteWorkers;
use days_executor::{Backend, RunImage, RunResult};

struct ThreadCountingAllocator;

thread_local! {
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
    static AT_EXECUTED: Cell<isize> = const { Cell::new(0) };
    static READBACK_GROWTH: Cell<isize> = const { Cell::new(0) };
}

fn record(delta: isize) {
    let _ = LIVE_BYTES.try_with(|live| live.set(live.get().wrapping_add(delta)));
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        record(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            record(new_size as isize - layout.size() as isize);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn phase(name: &'static str, _: usize) {
    let live = LIVE_BYTES.with(Cell::get);
    match name {
        "executed" => AT_EXECUTED.set(live),
        "read_back" => READBACK_GROWTH.set(live - AT_EXECUTED.get()),
        _ => {}
    }
}

fn smoke() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/smoke-simai.toml")
}

/// The readback growth of one run of `run` on `image`, and its result.
fn readback_growth(run: impl FnOnce() -> RunResult) -> (isize, RunResult) {
    days_executor::phase_marks::set_phase_hook_for_testing(Some(phase));
    let result = run();
    days_executor::phase_marks::set_phase_hook_for_testing(None);
    (READBACK_GROWTH.take(), result)
}

fn check(backend_name: &str, backend: Backend, run: impl Fn(RunImage<'_>) -> RunResult) {
    let image = compile_config(smoke()).expect("the smoke lowers");
    let state_bytes = {
        let before = LIVE_BYTES.with(Cell::get);
        let states = (image.host_states.clone(), image.switch_states.clone());
        let bytes = LIVE_BYTES.with(Cell::get) - before;
        drop(states);
        bytes
    };
    let (borrowed, borrowed_result) = readback_growth(|| run((&image).into()));
    drop(image);
    let (validated_image, _) =
        compile_config_validated(smoke(), RouteWorkers::available(), backend)
            .expect("the smoke lowers");
    let (validated, validated_result) = readback_growth(|| run(validated_image.into()));
    println!(
        "record=p18_readback_memory backend={backend_name} fixture=smoke state_bytes={state_bytes} \
         borrowed_readback_growth={borrowed} validated_readback_growth={validated}"
    );
    assert_eq!(
        validated_result, borrowed_result,
        "{backend_name}: the same result either way"
    );
    assert!(state_bytes > 0);
    assert!(
        borrowed - validated >= state_bytes,
        "{backend_name}: readback of a validated image grew by {validated} B against {borrowed} B \
         borrowed, less than the {state_bytes} B of states apart: it still clones them"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn metal_readback_moves_a_validated_images_states() {
    use days_executor::{MetalConfig, MetalExecutor, ObservationMode};
    let executor = MetalExecutor::new().expect("Metal executor");
    check("metal", Backend::Metal, |image| {
        let mut result = executor
            .run_with_observations(
                image,
                Some(5_000_000),
                MetalConfig {
                    capacity_caps: days::STOCK_CAPACITY_CAPS,
                    ..MetalConfig::default()
                },
                ObservationMode::Summary,
            )
            .expect("Metal runs the smoke")
            .result;
        result.diagnostics = None;
        result
    });
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_readback_moves_a_validated_images_states() {
    use days_executor::{CudaConfig, CudaExecutor, ObservationMode};
    let executor = CudaExecutor::new().expect("CUDA executor");
    check("cuda", Backend::Cuda, |image| {
        let mut result = executor
            .run_with_observations(
                image,
                Some(5_000_000),
                CudaConfig {
                    capacity_caps: days::STOCK_CAPACITY_CAPS,
                    ..CudaConfig::default()
                },
                ObservationMode::Summary,
            )
            .expect("CUDA runs the smoke")
            .result;
        result.diagnostics = None;
        result
    });
}
