//! P18 diet-host c9: identical routes share one copy in the device `routes` plane.
//!
//! The kernels read a flow's route through its `(offset, length)` row in the `flows` plane, so
//! the planners place each distinct route once. On the b4 rail fixture (2,048 flows, 128 GPUs)
//! the plane held every flow's forward and reverse route, 7,680 words; it now holds the 512 words
//! of the distinct routes only. The exact plan reports of both device planners (planner test hooks, no device
//! needed) must size the plane at exactly the distinct routes' words, and the host-only
//! projection (`size_default_device_plan`) must agree.
//!
//! Run: `cargo test -p days --features test,metal --test p18_route_plane` on the Mac,
//! `--features test,cuda` on a CUDA host.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]

use std::collections::BTreeSet;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{DeviceSizingReport, ObservationMode, SimulationImage};

fn b4() -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/b4-simai.toml");
    compile_config(&path).expect("b4 lowers")
}

/// Words of the distinct non-empty routes, forward and reverse.
fn distinct_route_words(image: &SimulationImage) -> usize {
    image
        .flows
        .iter()
        .flat_map(|flow| [flow.route.as_slice(), flow.reverse_route.as_slice()])
        .filter(|route| !route.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(<[_]>::len)
        .sum()
}

fn routes_bytes(report: &DeviceSizingReport) -> usize {
    report
        .planes
        .iter()
        .find(|plane| plane.name == "routes")
        .expect("the plan carries a routes plane")
        .bytes
}

fn check(backend: &str, image: &SimulationImage, report: &DeviceSizingReport) {
    let every = image
        .flows
        .iter()
        .map(|flow| flow.route.len() + flow.reverse_route.len())
        .sum::<usize>();
    let distinct = distinct_route_words(image);
    let bytes = routes_bytes(report);
    println!(
        "record=p18_route_plane backend={backend} fixture=b4 every_route_words={every} \
         distinct_route_words={distinct} routes_plane_bytes={bytes}"
    );
    assert!(distinct < every, "b4 repeats routes");
    assert_eq!(
        bytes,
        8 * distinct,
        "{backend}: the routes plane holds each distinct route once"
    );
    let projected = days_executor::size_default_device_plan(image).expect("b4 projects");
    assert_eq!(
        routes_bytes(&projected),
        bytes,
        "the host-only projection sizes the routes plane as the planner does"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn the_metal_routes_plane_stores_each_route_once() {
    use days_executor::{MetalConfig, size_metal_plan_for_testing};
    let image = b4();
    let report = size_metal_plan_for_testing(
        &image,
        None,
        MetalConfig::default(),
        ObservationMode::Summary,
    )
    .expect("b4 plans on Metal");
    check("metal", &image, &report);
}

#[cfg(feature = "cuda")]
#[test]
fn the_cuda_routes_plane_stores_each_route_once() {
    use days_executor::{CudaConfig, size_cuda_plan_for_testing};
    let image = b4();
    let report = size_cuda_plan_for_testing(
        &image,
        None,
        CudaConfig::default(),
        ObservationMode::Summary,
    )
    .expect("b4 plans on CUDA");
    check("cuda", &image, &report);
}
