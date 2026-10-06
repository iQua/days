//! P16 lane G2 (stagesize): the host projection's `tcp_state` plane against the production
//! planners' actual allocation (`days-gpu/evidence/P16/colldev-design.md` §0.2 item 3).
//!
//! `device-sizing-report` (`size_default_device_plan`) is the tool the design's fit argument rests
//! on. The production planners allocate `tcp_state` for every image: a receiver row and a ledger
//! row per flow, the TCP receive ranges and ledger records, then the stage region (P16 G1) and the
//! RoCE region (P15). The projection must report the same words.
#![cfg(all(
    feature = "test",
    any(
        feature = "cuda",
        feature = "cuda-planner-test",
        all(feature = "metal", target_vendor = "apple")
    )
))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    DeviceSizingReport, ObservationMode, SimulationImage, size_default_device_plan,
};

/// One image of each `tcp_state` composition: queue pairs only (with and without a window, and
/// with checkpoint-free resident packets), TCP with queue pairs, DCQCN rows, stages over RoCE, over
/// RoCE and TCP, and with compute stages, and an open-loop image.
const FIXTURES: &[&str] = &[
    "configs/p15/roce_gbn_lossy.toml",
    "configs/p15/roce_lossless_pfc.toml",
    "configs/p16/dcqcn_mlx_window.toml",
    "configs/p15/hostpfc_multi_qp_tcp.toml",
    "configs/p14/dcqcn_1s_zero_xoff.toml",
    "configs/p15/roce_ring_allreduce_lossless.toml",
    "configs/p15/roce_tcp_mixed_collectives.toml",
    "configs/p15/roce_compute_dag.toml",
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
];

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn tcp_state_words(report: &DeviceSizingReport) -> Option<usize> {
    report
        .planes
        .iter()
        .find(|plane| plane.name == "tcp_state")
        .map(|plane| plane.words)
}

// One entry per backend built into this test binary; each push is feature-gated.
#[allow(clippy::vec_init_then_push)]
fn exact_plans(image: &SimulationImage) -> Vec<(&'static str, DeviceSizingReport)> {
    #[allow(unused_mut)]
    let mut plans = Vec::new();
    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    plans.push((
        "CUDA",
        days_executor::size_cuda_plan_for_testing(
            image,
            None,
            days_executor::CudaConfig::default(),
            ObservationMode::Summary,
        )
        .expect("CUDA plan must size"),
    ));
    #[cfg(all(feature = "metal", target_vendor = "apple"))]
    plans.push((
        "Metal",
        days_executor::size_metal_plan_for_testing(
            image,
            None,
            days_executor::MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("Metal plan must size"),
    ));
    plans
}

#[test]
fn projected_tcp_state_is_the_planners_allocation() {
    for relative in FIXTURES {
        let image = lower(relative);
        let projected = size_default_device_plan(&image)
            .unwrap_or_else(|error| panic!("{relative} must project: {error}"));
        for (backend, exact) in exact_plans(&image) {
            let allocated = tcp_state_words(&exact)
                .unwrap_or_else(|| panic!("{backend} {relative}: the plan must carry tcp_state"));
            assert_eq!(
                tcp_state_words(&projected),
                Some(allocated),
                "{backend} {relative}: the projected tcp_state plane must be the planner's"
            );
        }
    }
}

/// Exact default plan bytes (Summary) of images with no stage and no windowed queue pair, MEASURED
/// on p16/colldev `da555a9` before P16 G2 (Metal on the Mac, CUDA on sim): rulings G7 and G8 leave
/// them byte for byte, because such an image builds no concurrency groups. The E-corpus plans
/// (E1, E2, E4, E5, E6), too large to allocate here, are compared word for word in the lane's
/// evidence (`evidence/P16/stagesize/stageless-pin/`).
const STAGELESS_PLAN_BYTES: &[(&str, usize, usize)] = &[
    (
        "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
        773_816,
        773_792,
    ),
    (
        "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
        1_417_280,
        1_417_256,
    ),
    ("configs/p14/dcqcn_1s_zero_xoff.toml", 260_992, 260_968),
    ("configs/p15/roce_lossless_pfc.toml", 854_400, 854_376),
    ("configs/p15/roce_gbn_lossy.toml", 360_928, 360_904),
    (
        "configs/p15/hostpfc_multi_qp_tcp.toml",
        2_656_176,
        2_656_152,
    ),
];

#[test]
fn stageless_windowless_plans_keep_their_bytes() {
    for &(relative, metal, cuda) in STAGELESS_PLAN_BYTES {
        let image = lower(relative);
        for (backend, exact) in exact_plans(&image) {
            let expected = if backend == "Metal" { metal } else { cuda };
            assert_eq!(exact.total_device_bytes, expected, "{backend} {relative}");
        }
    }
}

/// A RoCE AllGather ring of `ranks` hosts on one switch, `chained` times in a row: each
/// collective follows a compute group on the same hosts (compute -> AllGather -> compute ->
/// AllGather ...), so every host's stages form one chain of `chained * (ranks - 1)` RoCE stages.
/// Every AllGather sends the same chunk over the same ring routes.
fn chained_roce_rings(ranks: u64, chained: usize, window_bytes: u64) -> String {
    let switch = ranks;
    let edges = (0..ranks)
        .map(|host| format!("[{host}, {switch}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let hosts = (0..ranks)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sinks = (0..ranks)
        .map(|host| ((host + 1) % ranks).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let window = if window_bytes == 0 {
        String::new()
    } else {
        format!("window_bytes = {window_bytes}\n")
    };
    let mut config = format!(
        r#"
seed = 26
edges = [{edges}]
hosts = [{hosts}]
duration = 0.05

[switch]
port_rate = 1000000000
capacity = 300
discipline = "FIFO"
drop = "ECN_THRESHOLD"
ecn_threshold = 1.0

[link]
mode = "Pfc"

[link.pfc]
host_links = true
buffer_capacity = [0, 0, 0, 100000, 0, 0, 0, 0]
xoff = [0, 0, 0, 20000, 0, 0, 0, 0]
xon = [0, 0, 0, 10000, 0, 0, 0, 0]
"#
    );
    for step in 0..chained {
        let after = if step == 0 {
            String::new()
        } else {
            format!("after = \"ring{}\"\n", step - 1)
        };
        config.push_str(&format!(
            r#"
[[compute]]
name = "compute{step}"
hosts = [{hosts}]
duration_ns = 1000
{after}
[[collective]]
name = "ring{step}"
after = "compute{step}"
collective_type = "AllGather"
flow_type = "RoCE"
priority = 3
flow_count = {ranks}
sources = [{hosts}]
sinks = [{sinks}]

[collective.traffic]
initial_delay = 0.0
size = {size}
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}

[collective.traffic.dcqcn]
rate_gbps = 1.0
min_rate_gbps = 0.01
max_rate_gbps = 1.0
pacing_interval_ns = 1000

[collective.traffic.roce]
retransmit_timeout_ns = 0
feedback_priority = 0
{window}"#,
            size = 200_000 * ranks,
        ));
    }
    config
}

fn compile_text(config: &str) -> SimulationImage {
    let file = tempfile::NamedTempFile::new().expect("temporary fixture must open");
    std::fs::write(file.path(), config).expect("temporary fixture must be written");
    compile_config(file.path()).unwrap_or_else(|error| panic!("fixture must lower: {error}"))
}

fn plane_words(report: &DeviceSizingReport, name: &str) -> usize {
    report
        .planes
        .iter()
        .find(|plane| plane.name == name)
        .unwrap_or_else(|| panic!("{name} plane must exist"))
        .words
}

/// Ruling G7: a host's stages run one chain at a time, so lengthening the chain must not grow the
/// remote-staging or host-queue arenas. Three chained AllGathers plan exactly what one plans
/// (`2 × leaves × max` per host and slot, with one leaf per host and equal per-stage bounds); the
/// summed bound grew threefold.
#[test]
fn a_longer_stage_chain_plans_no_more_staging_or_queue() {
    let one = compile_text(&chained_roce_rings(4, 1, 0));
    let three = compile_text(&chained_roce_rings(4, 3, 0));
    let mut plans = vec![(
        "projection",
        size_default_device_plan(&one).expect("projection"),
        size_default_device_plan(&three).expect("projection"),
    )];
    for ((backend, one), (_, three)) in exact_plans(&one).into_iter().zip(exact_plans(&three)) {
        plans.push((backend, one, three));
    }
    for (backend, one, three) in plans {
        for plane in ["remote_staging", "queue_records"] {
            assert_eq!(
                plane_words(&three, plane),
                plane_words(&one, plane),
                "{backend}: three chained collectives must plan the {plane} of one"
            );
        }
    }
}

/// `configs/p15/hostpfc_multi_qp_tcp.toml` with a window on every queue pair: three pairs on host
/// 1 (and one each on hosts 2 and 3, beside host 2's TCP flow) pace 1 Gb/s each into a 1 Gb/s host
/// link, so host 1's queue grows until the windows bind (ruling G8).
fn windowed_multi_qp(window_bytes: u64) -> SimulationImage {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p15/hostpfc_multi_qp_tcp.toml");
    let config = std::fs::read_to_string(&path).expect("fixture must read");
    let windowed = config.replace(
        "feedback_priority = 0\n",
        &format!("feedback_priority = 0\nwindow_bytes = {window_bytes}\n"),
    );
    assert_eq!(
        windowed.matches("window_bytes").count(),
        5,
        "every queue pair has a window"
    );
    compile_text(&windowed)
}

/// `configs/p15/roce_ring_release_paused.toml` widened to `ranks` hosts on a switch line of the
/// same links (fix round 1, review F1): one RoCE RingAllReduce over the even hosts then the odd
/// ones, so every host sources a chain of `2 (ranks - 1)` windowless stage pairs of 100 packets,
/// and the background pair runs from the last host to host 0, keeping class 3 paused at its host
/// while stages there are released.
fn widened_release_paused(ranks: usize) -> SimulationImage {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p15/roce_ring_release_paused.toml");
    let config = std::fs::read_to_string(&path).expect("fixture must read");
    let list = |values: &[usize]| {
        values
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let edges = (0..ranks - 1)
        .map(|switch| format!("[{switch}, {}]", switch + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sources = (0..ranks)
        .step_by(2)
        .chain((1..ranks).step_by(2))
        .collect::<Vec<_>>();
    let sinks = sources[1..]
        .iter()
        .copied()
        .chain([sources[0]])
        .collect::<Vec<_>>();
    let mut widened = config;
    for (from, to) in [
        (
            "edges = [[0, 1], [1, 2], [2, 3]]".to_owned(),
            format!("edges = [{edges}]"),
        ),
        (
            "hosts = [0, 1, 2, 3]".to_owned(),
            format!("hosts = [{}]", list(&(0..ranks).collect::<Vec<_>>())),
        ),
        ("duration = 0.05".to_owned(), "duration = 0.2".to_owned()),
        ("flow_count = 4".to_owned(), format!("flow_count = {ranks}")),
        (
            "sources = [0, 2, 1, 3]".to_owned(),
            format!("sources = [{}]", list(&sources)),
        ),
        (
            "sinks = [2, 1, 3, 0]".to_owned(),
            format!("sinks = [{}]", list(&sinks)),
        ),
        (
            "size = 400000".to_owned(),
            format!("size = {}", 100_000 * ranks),
        ),
        (
            "graph = [[3, 0]]".to_owned(),
            format!("graph = [[{}, 0]]", ranks - 1),
        ),
    ] {
        assert_eq!(widened.matches(&from).count(), 1, "{from}");
        widened = widened.replace(&from, &to);
    }
    compile_text(&widened)
}

/// Capacity retries of the default device plan (design note §5.2), MEASURED on Metal (M5 Max) and
/// CUDA (sim, RTX A4500). Before ruling G8 the windowed image took four queue retries at host 1
/// (13 -> 28 -> 58 -> 118 -> 238 records); its three windowed pairs now plan their windows (154
/// records) and host 1 takes none. The one retry left is host 2's (56 -> 114 records): its TCP flow
/// on the paused class keeps today's horizon bound, which the ruling leaves to the typed retry.
/// The windowless P15 image keeps its eight queue retries (ruling G8 leaves it as it was).
///
/// Fix round 1 (review F1): the 16-rank paused ring's windowless stage pairs keep their summed
/// host-queue charge, as before ruling G7 (0 retries at `da555a9`; G7's one-chain charge had taken
/// 3 at host 15, 202 -> 1,630 records).
#[allow(dead_code)]
const PINNED_RETRIES: &[(&str, usize)] = &[
    ("hostpfc_multi_qp_window_50000", 1),
    ("hostpfc_multi_qp_tcp", 8),
    ("release_paused_ring16", 0),
];

#[allow(dead_code)]
fn retry_images() -> Vec<(&'static str, SimulationImage)> {
    vec![
        ("hostpfc_multi_qp_window_50000", windowed_multi_qp(50_000)),
        (
            "hostpfc_multi_qp_tcp",
            lower("configs/p15/hostpfc_multi_qp_tcp.toml"),
        ),
        ("release_paused_ring16", widened_release_paused(16)),
    ]
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations, run_scalar};

    #[test]
    fn metal_queue_pair_images_pin_their_retries() {
        let mut retries = Vec::new();
        for (name, image) in super::retry_images() {
            let run = run_metal_with_observations(
                &image,
                None,
                MetalConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            let mut expected = run_scalar(&image, None).expect("scalar oracle must run");
            expected.diagnostics = None;
            assert_eq!(run.result, expected, "{name}: Metal must equal Scalar");
            retries.push((name, run.capacity_retry_trace.len()));
        }
        eprintln!("record=queue_pair_retries backend=metal {retries:?}");
        assert_eq!(retries, super::PINNED_RETRIES);
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations, run_scalar};

    #[test]
    fn cuda_queue_pair_images_pin_their_retries() {
        let mut retries = Vec::new();
        for (name, image) in super::retry_images() {
            let run = run_cuda_with_observations(
                &image,
                None,
                CudaConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            let mut expected = run_scalar(&image, None).expect("scalar oracle must run");
            expected.diagnostics = None;
            assert_eq!(run.result, expected, "{name}: CUDA must equal Scalar");
            retries.push((name, run.capacity_retry_trace.len()));
        }
        eprintln!("record=queue_pair_retries backend=cuda {retries:?}");
        assert_eq!(retries, super::PINNED_RETRIES);
    }
}
