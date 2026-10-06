//! P16 H4 (resumebits): the device PFC RESUME scan walks per-class parked bitsets (ruling G9).
//!
//! The images are P16 G2's RESUME-scan gate (`evidence/P16/stagesize/tooling/gen_resume_gate.py`
//! in days-gpu, ported here): the P15 RoCE fabric (a switch line, one host per switch, 1 Gb/s,
//! lossless classes with host-link PFC, feedback on class 0, no ECN marking) on five switches,
//! with `k` concurrent RoCE RingAllReduce collectives over the ring 0, 2, 4, 1, 3 and a fixed
//! 20,000 B per stage. Each host holds `8k` stage queue pairs, about `k` of them released at once,
//! and its host link keeps pausing however large `k` is.
//!
//! - Identity: the gate images, a two-class variant, an 8-pair variant with a 20 us retransmission
//!   timeout (whose timeouts leave stale parked bits), and checkpoints taken just before RESUMEs
//!   that restart two or more pairs, on Metal and CUDA against Scalar, in Full and Summary.
//! - Scan work (test hooks): the device counts the queue pairs each unpausing RESUME examines.
//!   Before G9 the scan walked the host's whole queue-pair list, `8k` pairs per RESUME; with the
//!   bitsets it examines the pairs parked since the class's last RESUME, independent of `k`.

#![cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]

use std::collections::BTreeMap;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    FlowGeneratorKind, MechanismTransitionRecord, NodeKind, ObservationMode, PfcControlAction,
    RoceSenderKind, RoceTransitionRecord, RunResult, SimulationImage, run_scalar_with_observations,
};

const RANKS: usize = 5;
const PACKET_BYTES: u64 = 1_000;
const CHUNK_PACKETS: u64 = 20;

/// The gate image with `k` collectives; collective `i` takes data class `classes[i % len]`, and
/// every class in `classes` is lossless with host-link PFC.
fn gate_toml(k: usize, classes: &[u8], rto_ns: u64) -> String {
    let per_class = |value: u64| {
        let words = (0..8_u8)
            .map(|class| {
                if classes.contains(&class) {
                    value.to_string()
                } else {
                    "0".to_owned()
                }
            })
            .collect::<Vec<_>>();
        format!("[{}]", words.join(", "))
    };
    let mut text = format!(
        "# P16 H4 RESUME-scan gate: {k} RingAllReduce collectives, {} stage queue pairs per host.\n\
         seed = 42\n\
         edges = [[0, 1], [1, 2], [2, 3], [3, 4]]\n\
         hosts = [0, 1, 2, 3, 4]\n\
         duration = 2.0\n\n\
         [switch]\nport_rate = 1000000000\ncapacity = 300\ndiscipline = \"FIFO\"\n\
         drop = \"ECN_THRESHOLD\"\necn_threshold = 1.0\n\n\
         [link]\nmode = \"Pfc\"\n\n\
         [link.pfc]\nhost_links = true\nbuffer_capacity = {}\nxoff = {}\nxon = {}\n",
        2 * k * (RANKS - 1),
        per_class(100_000),
        per_class(20_000),
        per_class(10_000),
    );
    for index in 0..k {
        text.push_str(&format!(
            "\n[[collective]]\nname = \"allreduce{index}\"\ncollective_type = \"RingAllReduce\"\n\
             flow_type = \"RoCE\"\npriority = {priority}\nflow_count = 5\n\
             sources = [0, 2, 4, 1, 3]\nsinks = [2, 4, 1, 3, 0]\n\n\
             [collective.traffic]\ninitial_delay = 0.0\nsize = {size}\n\
             arr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\n\
             pkt_size_dist = {{ type = \"DiscreteUniform\", low = {PACKET_BYTES}, high = {PACKET_BYTES} }}\n\n\
             [collective.traffic.dcqcn]\nrate_gbps = 1.0\nmin_rate_gbps = 0.01\nmax_rate_gbps = 1.0\n\
             g = 0.00390625\nai_rate_gbps = 0.005\nhai_rate_gbps = 0.05\nrp_timer_ns = 50000\n\
             pacing_interval_ns = 1000\n\n\
             [collective.traffic.roce]\nretransmit_timeout_ns = {rto_ns}\nfeedback_priority = 0\n",
            priority = classes[index % classes.len()],
            size = CHUNK_PACKETS * PACKET_BYTES * RANKS as u64,
        ));
    }
    text
}

/// Lowers the gate image with `k` collectives over `classes`. The TOML is written under Cargo's
/// test scratch directory, to a per-thread file renamed into place, so parallel tests never read a
/// partial file.
fn gate(k: usize, classes: &[u8]) -> SimulationImage {
    gate_with_timeout(k, classes, 0)
}

/// The gate image with a retransmission timeout of `rto_ns` on every queue pair (0: none).
fn gate_with_timeout(k: usize, classes: &[u8], rto_ns: u64) -> SimulationImage {
    let directory = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("p16_resume_bitsets");
    std::fs::create_dir_all(&directory).expect("scratch directory");
    let label = classes
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join("_");
    let path = directory.join(format!("resume_gate_k{k}_c{label}_rto{rto_ns}.toml"));
    let partial = directory.join(format!(
        "resume_gate_k{k}_c{label}_rto{rto_ns}.{:?}.partial",
        std::thread::current().id()
    ));
    std::fs::write(&partial, gate_toml(k, classes, rto_ns)).expect("write the gate image");
    std::fs::rename(&partial, &path).expect("publish the gate image");
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    let mut expected =
        run_scalar_with_observations(image, horizon, mode).expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

fn checkpoint_image(original: &SimulationImage, checkpoint: &RunResult) -> SimulationImage {
    let mut image = original.clone();
    image.host_states.clone_from(&checkpoint.host_states);
    image.switch_states.clone_from(&checkpoint.switch_states);
    image
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    image.initial_events.clone_from(&checkpoint.pending_events);
    image
}

/// The queue pairs of the busiest host (every gate host holds the same number).
fn pairs_per_host(image: &SimulationImage) -> usize {
    image
        .host_states
        .iter()
        .map(|host| {
            host.generators
                .iter()
                .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
                .count()
        })
        .max()
        .unwrap_or(0)
}

/// Scalar's RESUME work in one run: every RESUME that unpaused a class at a host (a host
/// `PfcControl` resume whose controller set empties), and the queue pairs each one restarted
/// (its `Resume` sender records), by the RESUME's event key.
#[derive(Debug, Default)]
struct ResumeWork {
    /// `(time, data class, restarts)` per unpausing RESUME, in event order.
    resumes: Vec<(u64, u8, usize)>,
}

impl ResumeWork {
    fn of(image: &SimulationImage) -> Self {
        let run = run_scalar_with_observations(image, None, ObservationMode::Full)
            .expect("scalar oracle must run");
        let egress = run
            .host_states
            .iter()
            .map(|host| host.egress_link)
            .collect::<Vec<_>>();
        let mut unpausing = Vec::new();
        let mut restarts = BTreeMap::<_, usize>::new();
        for record in &run
            .diagnostics
            .as_ref()
            .expect("full diagnostics")
            .mechanism_transitions
        {
            match record {
                MechanismTransitionRecord::PfcControl(control)
                    if egress.contains(&control.controlled_link)
                        && control.action == PfcControlAction::Resume
                        && control.after_controllers.is_empty()
                        && !control.before_controllers.is_empty() =>
                {
                    unpausing.push((control.key, control.priority));
                }
                MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender))
                    if sender.kind == RoceSenderKind::Resume =>
                {
                    *restarts.entry(sender.key).or_default() += 1;
                }
                _ => {}
            }
        }
        let resumes = unpausing
            .iter()
            .map(|(key, class)| {
                (
                    key.time_ns,
                    *class,
                    restarts.get(key).copied().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            resumes.iter().map(|(_, _, count)| count).sum::<usize>(),
            restarts.values().sum::<usize>(),
            "every restart belongs to an unpausing RESUME at a host"
        );
        Self { resumes }
    }

    fn restarts(&self) -> usize {
        self.resumes.iter().map(|(_, _, count)| count).sum()
    }
}

/// Checkpoints of `image` taken just before the first `limit` RESUMEs that restart two or more
/// pairs: the class is still paused there, so the image carries a parked list of two or more pairs
/// the device's bitsets must start from.
fn mid_pause_checkpoints(
    label: &str,
    image: &SimulationImage,
    work: &ResumeWork,
    limit: usize,
) -> Vec<(String, SimulationImage)> {
    let mut horizons = work
        .resumes
        .iter()
        .filter(|(_, _, restarts)| *restarts >= 2)
        .map(|(time, _, _)| *time)
        .collect::<Vec<_>>();
    horizons.dedup();
    horizons
        .into_iter()
        .take(limit)
        .map(|horizon| {
            let prefix = run_scalar_with_observations(image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            (
                format!("{label}@{horizon}"),
                checkpoint_image(image, &prefix),
            )
        })
        .collect()
}

/// The largest parked list of one class at one host in `image`.
fn max_parked(image: &SimulationImage) -> usize {
    image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
        .filter_map(|node| image.host_states[node.state_slot as usize].pfc.as_deref())
        .flat_map(|pfc| pfc.pause_parked.iter().map(std::collections::BTreeSet::len))
        .max()
        .unwrap_or(0)
}

/// The identity images: the 256-pair gate, the two-class gate, and mid-pause checkpoints of both.
fn identity_images() -> Vec<(String, SimulationImage)> {
    let mut images = Vec::new();
    for (label, image) in [
        ("gate_k32".to_owned(), gate(32, &[3])),
        ("gate_k8_two_classes".to_owned(), gate(8, &[3, 4])),
    ] {
        let work = ResumeWork::of(&image);
        images.extend(mid_pause_checkpoints(&label, &image, &work, 2));
        images.insert(0, (label, image));
    }
    images.push(("gate_k1_rto20us".to_owned(), stale_bit_gate()));
    images
}

/// The 8-pair gate with a 20 us retransmission timeout: timeouts restart pause-parked pairs while
/// their class is still paused, so some RESUMEs find a set bit whose pair is no longer parked
/// (its restarted tick is pending) and skip it. The scan-work test checks the skips happen.
fn stale_bit_gate() -> SimulationImage {
    gate_with_timeout(1, &[3], 20_000)
}

/// The gate images exercise what the identity and scan-work tests rely on: unpausing RESUMEs that
/// restart two or more pairs (so the walk order matters), a host whose two data classes are both
/// paused and resumed (so the per-class indexing matters), and mid-pause checkpoints with two or
/// more parked pairs of one class at one host (so the plan's bitsets start from Scalar's list).
#[test]
fn gate_images_exercise_multi_restart_resumes_two_classes_and_parked_checkpoints() {
    let one_class = gate(32, &[3]);
    assert_eq!(pairs_per_host(&one_class), 256);
    let work = ResumeWork::of(&one_class);
    assert!(!work.resumes.is_empty(), "the 256-pair gate pauses");
    assert!(
        work.resumes.iter().any(|(_, _, restarts)| *restarts >= 2),
        "a RESUME restarts two or more pairs"
    );

    let two_classes = gate(8, &[3, 4]);
    let work = ResumeWork::of(&two_classes);
    for class in [3, 4] {
        assert!(
            work.resumes
                .iter()
                .any(|(_, resumed, restarts)| *resumed == class && *restarts >= 1),
            "a RESUME of class {class} restarts a pair"
        );
    }

    let checkpoints = identity_images()
        .into_iter()
        .filter(|(name, _)| name.contains('@'))
        .collect::<Vec<_>>();
    assert_eq!(checkpoints.len(), 4, "two checkpoints per gate image");
    for (name, image) in &checkpoints {
        assert!(
            max_parked(image) >= 2,
            "{name}: a host holds two or more parked pairs of one class"
        );
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, RunResult, run_cuda_with_observations};

    use super::{identity_images, scalar};

    pub(super) fn run(
        image: &days_executor::SimulationImage,
        horizon: Option<u64>,
        mode: ObservationMode,
        (streams_enabled, round_threads_per_block): (bool, usize),
    ) -> RunResult {
        run_cuda_with_observations(
            image,
            horizon,
            CudaConfig {
                streams_enabled,
                round_threads_per_block,
                ..CudaConfig::default()
            },
            mode,
        )
        .unwrap_or_else(|error| panic!("cuda run: {error}"))
        .result
    }

    #[test]
    fn cuda_resume_gate_images_and_checkpoints_match_scalar_in_full_and_summary() {
        for (name, image) in identity_images() {
            for mode in [ObservationMode::Full, ObservationMode::Summary] {
                for horizon in [None, Some(image.stop_time_ns / 2)] {
                    let expected = scalar(&image, horizon, mode);
                    for config in [(true, 256), (false, 32)] {
                        assert_eq!(
                            run(&image, horizon, mode, config),
                            expected,
                            "{name} {mode:?} horizon={horizon:?} {config:?}"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, RunResult, run_metal_with_observations};

    use super::{identity_images, scalar};

    pub(super) fn run(
        image: &days_executor::SimulationImage,
        horizon: Option<u64>,
        mode: ObservationMode,
        (streams_enabled, round_threads_per_threadgroup): (bool, usize),
    ) -> RunResult {
        run_metal_with_observations(
            image,
            horizon,
            MetalConfig {
                streams_enabled,
                round_threads_per_threadgroup,
                ..MetalConfig::default()
            },
            mode,
        )
        .unwrap_or_else(|error| panic!("metal run: {error}"))
        .result
    }

    #[test]
    fn metal_resume_gate_images_and_checkpoints_match_scalar_in_full_and_summary() {
        for (name, image) in identity_images() {
            for mode in [ObservationMode::Full, ObservationMode::Summary] {
                for horizon in [None, Some(image.stop_time_ns / 2)] {
                    let expected = scalar(&image, horizon, mode);
                    for config in [(true, 256), (false, 32)] {
                        assert_eq!(
                            run(&image, horizon, mode, config),
                            expected,
                            "{name} {mode:?} horizon={horizon:?} {config:?}"
                        );
                    }
                }
            }
        }
    }
}

/// The device RESUME scans' work at 256 and 768 queue pairs per host, from the test-hook
/// counters, against Scalar's unpausing RESUMEs and restarts (ruling G9).
///
/// Each unpausing RESUME reads its class's `ceil(Q/64)` bitset words and examines only the pairs
/// whose bits are set: the pairs parked since the class's last RESUME. A bit is set when a paused
/// tick parks its pair and cleared by the RESUME's walk, so a pair an ACK, NACK or timeout restarted
/// after it parked is examined once and skipped. The skips per RESUME therefore stay bounded independent of `Q`;
/// before G9 every RESUME examined all `Q` pairs, skipping about `Q - 1`.
#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
fn assert_scan_work(backend: &str, run: impl Fn(&SimulationImage) -> RunResult) {
    for k in [32, 96] {
        let image = gate(k, &[3]);
        let pairs = pairs_per_host(&image);
        assert_eq!(pairs, 8 * k);
        let work = ResumeWork::of(&image);
        let expected = scalar(&image, None, ObservationMode::Full);
        assert_eq!(run(&image), expected, "{backend} gate k={k}");
        let counts = days_executor::take_resume_scan_counts_for_testing()
            .expect("the device run recorded its RESUME-scan counters");
        let resumes = work.resumes.len() as u64;
        let restarts = work.restarts() as u64;
        eprintln!(
            "record=resume_scan backend={backend} pairs_per_host={pairs} unpausing_resumes={resumes} \
             scalar_restarts={restarts} device_resumes={} pairs_examined={} words_read={} \
             skipped={} mean_skipped_per_resume={:.3}",
            counts.resumes,
            counts.pairs_examined,
            counts.words_read,
            counts.pairs_examined.saturating_sub(restarts),
            counts.pairs_examined.saturating_sub(restarts) as f64 / resumes.max(1) as f64,
        );
        assert_eq!(
            counts.resumes, resumes,
            "{backend} k={k}: one scan per RESUME"
        );
        assert!(
            counts.pairs_examined >= restarts,
            "{backend} k={k}: every restarted pair is examined"
        );
        assert!(
            counts.pairs_examined - restarts <= resumes,
            "{backend} k={k}: at most one skip per RESUME on average, not about Q - 1 = {}: \
             {} examined for {restarts} restarts over {resumes} RESUMEs",
            pairs - 1,
            counts.pairs_examined,
        );
        assert_eq!(
            counts.words_read,
            resumes * pairs.div_ceil(64) as u64,
            "{backend} k={k}: each RESUME reads its class's ceil(Q/64) bitset words"
        );
    }

    // A stale bit (a pair a timeout restarted after it parked) is examined once and skipped, and
    // the result is still Scalar's.
    let image = stale_bit_gate();
    let work = ResumeWork::of(&image);
    assert_eq!(
        run(&image),
        scalar(&image, None, ObservationMode::Full),
        "{backend} stale-bit gate"
    );
    let counts = days_executor::take_resume_scan_counts_for_testing()
        .expect("the device run recorded its RESUME-scan counters");
    let (resumes, restarts) = (work.resumes.len() as u64, work.restarts() as u64);
    eprintln!(
        "record=resume_scan backend={backend} image=stale_bit_gate unpausing_resumes={resumes} \
         scalar_restarts={restarts} device_resumes={} pairs_examined={} words_read={}",
        counts.resumes, counts.pairs_examined, counts.words_read,
    );
    assert_eq!(counts.resumes, resumes);
    assert!(
        counts.pairs_examined > restarts && counts.pairs_examined - restarts <= resumes,
        "{backend}: the stale-bit gate skips some set bits, at most one per RESUME on average: \
         {} examined for {restarts} restarts over {resumes} RESUMEs",
        counts.pairs_examined,
    );
}

#[cfg(feature = "cuda-test-hooks")]
#[test]
fn cuda_resume_scan_work_is_independent_of_queue_pairs_per_host() {
    assert_scan_work("cuda", |image| {
        cuda::run(image, None, ObservationMode::Full, (true, 256))
    });
}

#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[test]
fn metal_resume_scan_work_is_independent_of_queue_pairs_per_host() {
    assert_scan_work("metal", |image| {
        metal::run(image, None, ObservationMode::Full, (true, 256))
    });
}
