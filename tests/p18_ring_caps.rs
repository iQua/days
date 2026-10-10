//! P18 diet-host c1 (host half): the stock channel cap is sized from observed ring occupancy, and
//! it bounds the data rings only.
//!
//! P17 M5 reconstructed every channel ring's high-water on the flagship (1 ms window), M2s and the
//! MoE smoke (`days-gpu/evidence/P17/scale/measure/tables/m5.md`): data rings peaked at 251
//! events (flagship), 101 (M2s, smoke), against derived capacities of 327 to 1,005 slots, 0.62%
//! of the flagship's 13.0 GB of data rings used. Ruling 4 sets the stock cap to the next power of
//! two at or above twice the largest high-water: 2 x 251 = 502, so 512. Capacity is refuse-or-run,
//! never semantics: a ring that needs more fails its attempt before committing an exchange and
//! the retry grows it, so the cap changes plan bytes and never complete state.
//!
//! The stage-notify lanes were not observable in M5, so the cap leaves them alone: a lane holds at
//! most its host pair's unfired notifies, a count lowering fixes, and keeps that capacity.
//!
//! **What is measured.** The exact plan's channel-ring capacity levels (planner test hooks, no
//! device needed) on the MoE smoke: 18,944 two-slot PFC control lanes, 256 and 128 notify lanes
//! of 98 and 287 slots, and 19,072 data rings derived at 1,005 slots.
//!
//! Run: `cargo test --release -p days --features test,metal --test p18_ring_caps` on the Mac,
//! `--features test,cuda` on a CUDA host.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    ChannelStreamCapacityLevel, DeviceCapacityCaps, DeviceSizingReport, ObservationMode,
    SimulationImage,
};

fn smoke() -> SimulationImage {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/smoke-simai.toml");
    compile_config(&path).expect("the smoke lowers")
}

fn levels(report: &DeviceSizingReport) -> Vec<(usize, usize)> {
    report
        .channel_stream_capacity_distribution
        .iter()
        .map(
            |&ChannelStreamCapacityLevel {
                 capacity,
                 stream_count,
             }| (capacity, stream_count),
        )
        .collect()
}

/// The smoke's ring levels under a channel cap of `cap`, from `plan`.
fn check(backend: &str, plan: impl Fn(DeviceCapacityCaps) -> DeviceSizingReport) {
    // Ruling 4: twice the largest M5 data-ring high-water, rounded up to a power of two.
    assert_eq!(
        days::STOCK_CAPACITY_CAPS.channel_events_per_stream,
        Some((2 * 251_usize).next_power_of_two())
    );
    let stock = levels(&plan(days::STOCK_CAPACITY_CAPS));
    println!("record=p18_ring_caps backend={backend} caps=stock levels={stock:?}");
    assert_eq!(
        stock,
        [(2, 18_944), (98, 256), (287, 128), (512, 19_072)],
        "{backend}: the stock cap bounds the data rings at 512 and leaves the lanes"
    );
    let tight = levels(&plan(DeviceCapacityCaps {
        channel_events_per_stream: Some(64),
        ..days::STOCK_CAPACITY_CAPS
    }));
    println!("record=p18_ring_caps backend={backend} caps=64 levels={tight:?}");
    assert_eq!(
        tight,
        [(2, 18_944), (64, 19_072), (98, 256), (287, 128)],
        "{backend}: a cap below a notify lane's count leaves the lane at its count"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn metal_caps_the_data_rings_and_keeps_the_notify_lanes() {
    use days_executor::{MetalConfig, size_metal_plan_for_testing};
    let image = smoke();
    check("metal", |capacity_caps| {
        size_metal_plan_for_testing(
            &image,
            None,
            MetalConfig {
                capacity_caps,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("the smoke plans on Metal")
    });
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_caps_the_data_rings_and_keeps_the_notify_lanes() {
    use days_executor::{CudaConfig, size_cuda_plan_for_testing};
    let image = smoke();
    check("cuda", |capacity_caps| {
        size_cuda_plan_for_testing(
            &image,
            None,
            CudaConfig {
                capacity_caps,
                ..CudaConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("the smoke plans on CUDA")
    });
}
