//! P16 PFC service identity: the depth-independent PFC service paths serve exactly the packets the
//! whole-queue plan served, under every scheduling discipline, from the start of a run and from
//! checkpoints taken while a class is paused.
//!
//! The images are P14's three-source PFC incast (`tests/p14_device_pfc.rs`) under each of the five
//! disciplines: priorities 3 and 1 are lossless and pause, and priority 0 is not PFC-controlled, so
//! its packets are served past paused heads. For each discipline the anchor is the list of
//! fingerprints of the Scalar full-observation run and of the runs resumed from every
//! eighth-of-the-run checkpoint at which a switch queue has a paused class (a resumed run builds
//! its FIFO class orders from the paused state it starts in). The anchors were recorded at
//! `feat/p16` `6454583`, before the change, with this file; the fingerprint is the FNV-1a64 over
//! the pretty `Debug` rendering that `days` prints as `result_fnv1a64`. The CPU executor at one
//! and three workers must equal Scalar on every one of those runs.
//!
//! Run: `cargo test -p days --test p16_pfc_service_identity` (default matrix, any profile).

use std::fs;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, ObservationMode, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_with_observations,
};

/// Per discipline: (bytes, FNV-1a64) of the full run, then of each paused-checkpoint resumption.
const ANCHORS: [(&str, &[(u64, u64)]); 5] = [
    (
        "FIFO",
        &[
            (641_403, 0x9513_c6fc_ff79_fc5b),
            (545_896, 0x1d91_02e9_d0f7_e32d),
            (480_269, 0xc4bb_eb36_dc82_aa85),
            (396_215, 0xddd2_1aaa_5223_81cf),
            (335_027, 0xce80_75fa_3453_a2d0),
            (262_936, 0xf0ff_5826_e152_2f78),
            (193_194, 0xbcd4_e4af_0958_357a),
            (142_490, 0xc551_f76b_e48e_e0fd),
        ],
    ),
    (
        "SP",
        &[
            (512_742, 0xbacc_e48d_3815_9ec9),
            (438_167, 0xea5b_9459_5504_2d53),
            (380_392, 0xdcbe_6277_6b1f_711a),
            (321_764, 0x232b_3d20_2224_bf1e),
            (260_576, 0xef6e_1306_7d47_e8a4),
            (207_429, 0x5f7a_16fe_422d_4739),
            (155_497, 0x4df5_3efc_e43a_b429),
            (129_869, 0xac2d_5e51_235b_931b),
        ],
    ),
    (
        "WFQ",
        &[
            (852_384, 0x001d_0777_0e6b_0d99),
            (759_174, 0xdffc_9945_f359_10b3),
            (677_804, 0xdbf9_3e7b_cab7_084c),
            (585_631, 0x2c27_5075_e441_01ab),
            (483_188, 0x5908_9dac_e8cc_8602),
            (397_954, 0xb6d8_87bb_815b_1785),
            (307_718, 0x6435_8aa3_b082_29af),
            (225_716, 0x3516_d828_74fb_be31),
        ],
    ),
    (
        "DRR",
        &[
            (4_448_984, 0xb603_161a_261e_fa7a),
            (4_223_693, 0x5e0b_bfc2_d4cc_9068),
            (3_902_813, 0x384e_3009_5f9f_a746),
            (3_415_688, 0xf178_d06f_2d5d_c0c1),
            (2_784_479, 0x6870_782f_969b_18fd),
            (2_142_245, 0xbd90_19b3_f616_cd07),
            (1_496_405, 0xdea2_20e1_e609_78a7),
            (813_532, 0xc29f_eb5e_075a_a8e8),
        ],
    ),
    (
        "WRR",
        &[
            (4_561_125, 0x2f9a_c0ef_65eb_dd4d),
            (4_321_342, 0xb24f_e1dd_d850_e8ff),
            (3_977_703, 0x6d04_bdd3_8198_3ee8),
            (3_510_958, 0x9cab_87f0_3dd1_e5f1),
            (2_857_187, 0xd600_ad00_fcad_8bc2),
            (2_222_538, 0x126d_596d_74b7_ffc9),
            (1_536_258, 0xb6ba_73ae_1f6f_5a48),
            (835_627, 0xe788_087e_8c56_3624),
        ],
    ),
];

fn fingerprint(value: &impl std::fmt::Debug) -> (u64, u64) {
    let text = format!("{value:#?}");
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (text.len() as u64, hash)
}

/// Three sources converge on switch 2, whose egress to switch 3 is the bottleneck. Priorities 3
/// and 1 are lossless (nonzero XOFF); priority 0 is not PFC-controlled, so its packets stay
/// eligible while the others are paused. (`tests/p14_device_pfc.rs`, unchanged.)
fn incast(discipline: &str) -> SimulationImage {
    let directory = tempfile::TempDir::new().expect("temporary directory");
    let path = directory.path().join("pfc_incast.toml");
    let flow = |source: u32, priority: u8, size: u64, delay: &str| {
        format!(
            r#"
[[flow]]
flow_type = "PacketDistribution"
priority = {priority}
graph = [[{source}, 3]]

[flow.traffic]
initial_delay = {delay}
size = {size}
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}
"#
        )
    };
    let config = format!(
        r#"
seed = 14
edges = [[0, 2], [1, 2], [2, 3]]
hosts = [0, 1, 2, 3]
duration = 0.001

[switch]
port_rate = 1_000_000_000
capacity = 1000
weights = [3, 1, 2]
priorities = [3, 2, 1]
discipline = "{discipline}"
drop = "TailDrop"

[link]
mode = "Pfc"

[link.pfc]
xoff = [0, 3000, 0, 3000, 0, 0, 0, 0]
xon = [0, 1500, 0, 1500, 0, 0, 0, 0]
pause_quanta = [1, 1, 1, 1, 1, 1, 1, 1]
buffer_capacity = [0, 8000, 0, 8000, 0, 0, 0, 0]
{}{}{}{}"#,
        flow(0, 3, 100_000, "0.0"),
        flow(1, 1, 100_000, "0.000002"),
        flow(0, 0, 60_000, "0.000001"),
        flow(2, 3, 60_000, "0.000003"),
    );
    fs::write(&path, config).expect("scenario must be written");
    compile_config(&path).unwrap_or_else(|error| panic!("{discipline} incast must lower: {error}"))
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

fn switch_queue_paused(result: &RunResult) -> bool {
    result
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .filter_map(|queue| queue.pfc.as_ref())
        .any(|pfc| pfc.paused_by_controller.iter().any(|set| !set.is_empty()))
}

/// The Scalar full-observation run of `image`, which the CPU executor at one and three workers
/// must equal.
fn scalar_equal_to_cpu(name: &str, image: &SimulationImage) -> RunResult {
    let scalar = run_scalar_with_observations(image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name}: Scalar run: {error}"));
    for workers in [1, 3] {
        let cpu = run_cpu_with_observations(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("{name}: CPU {workers} run: {error}"));
        assert_eq!(cpu.result, scalar, "{name}: CPU {workers} diverged");
    }
    scalar
}

#[test]
fn incasts_and_paused_checkpoints_match_their_frozen_anchors() {
    for (discipline, anchor) in ANCHORS {
        let image = incast(discipline);
        let mut fingerprints = vec![fingerprint(&scalar_equal_to_cpu(discipline, &image))];
        for step in 1..8 {
            let horizon = image.stop_time_ns / 8 * step;
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("the checkpoint prefix runs");
            if !switch_queue_paused(&prefix) {
                continue;
            }
            let name = format!("{discipline}@{horizon}");
            let resumed = checkpoint_image(&image, &prefix);
            fingerprints.push(fingerprint(&scalar_equal_to_cpu(&name, &resumed)));
        }
        println!(
            "record=p16_pfc_service_identity discipline={discipline} fingerprints={fingerprints:x?}"
        );
        assert!(
            fingerprints.len() >= 2,
            "{discipline}: no checkpoint with a paused switch queue"
        );
        assert_eq!(fingerprints, anchor, "{discipline}");
    }
}

/// The incasts exercise the first-eligible plan where the anchors hold it: under FIFO and WFQ,
/// decisions serve packets behind a paused head (the FIFO ones through the class order, the WFQ
/// ones by search), and FIFO reads stay within the per-decision budget of
/// `tests/p16_pfc_service_budget.rs`, from the start and from paused checkpoints, where the FIFO
/// class order is built from the starting state.
#[cfg(feature = "test")]
#[test]
fn incasts_serve_past_paused_heads_without_reading_the_queue() {
    use days_executor::scalar::run_scalar_counting_pfc_service_for_testing;

    for discipline in ["FIFO", "SP", "WFQ"] {
        let image = incast(discipline);
        let mut images = vec![(discipline.to_owned(), image.clone())];
        for step in 1..8 {
            let horizon = image.stop_time_ns / 8 * step;
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("the checkpoint prefix runs");
            if switch_queue_paused(&prefix) {
                images.push((
                    format!("{discipline}@{horizon}"),
                    checkpoint_image(&image, &prefix),
                ));
            }
        }
        for (name, image) in images {
            let (_, counts) =
                run_scalar_counting_pfc_service_for_testing(&image, ObservationMode::Full)
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
            println!("record=p16_pfc_service_coverage case={name} {counts:?}");
            // The static-priority incast never has an eligible packet behind a paused head.
            assert!(
                discipline == "SP" || counts.past_head > 0,
                "{name}: no decision served past a paused head"
            );
            if discipline == "FIFO" {
                assert!(
                    counts.reads <= 2 * counts.decisions,
                    "{name}: {} reads over {} decisions",
                    counts.reads,
                    counts.decisions
                );
            }
        }
    }
}
