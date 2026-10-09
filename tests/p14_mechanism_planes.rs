//! P14 Lane B Acceptance 5: DCQCN and PFC state costs nothing in a production device plan when the
//! image carries none.
//!
//! The hooks build the real production plan and measure what it spends on the mechanisms: the PFC
//! region appended to the scheduler plane and the DCQCN-marked per-flow receiver rows. The E5/E6
//! evaluation fixtures, which the zero-cost A/B measures, must plan zero words for both. The P14
//! fixtures must plan a nonzero amount, so the measurement is not vacuous.
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
use days_executor::{MechanismPlaneWords, SimulationImage};

const EVALUATION: [&str; 8] = [
    "e5_wide_k32_q200.toml",
    "e5_wide_k32_q200_cubic.toml",
    "e5_wide_k32_q256.toml",
    "e5_legacy_k4_loss.toml",
    "e6_cbr_k32_load_01.toml",
    "e6_cbr_k32_load_10.toml",
    "e6_cbr_k32_load_30.toml",
    "e6_cbr_k32_load_60.toml",
];

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

// One entry per backend built into this test binary; each push is feature-gated.
#[allow(clippy::vec_init_then_push)]
fn measure(image: &SimulationImage) -> Vec<(&'static str, MechanismPlaneWords)> {
    #[allow(unused_mut)]
    let mut measured = Vec::new();
    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    measured.push((
        "CUDA",
        days_executor::mechanism_plane_words_cuda_for_testing(
            image,
            days_executor::CudaConfig::default(),
        )
        .expect("CUDA plan must size"),
    ));
    #[cfg(all(feature = "metal", target_vendor = "apple"))]
    measured.push((
        "Metal",
        days_executor::mechanism_plane_words_metal_for_testing(
            image,
            days_executor::MetalConfig::default(),
        )
        .expect("Metal plan must size"),
    ));
    measured
}

#[test]
fn evaluation_fixtures_plan_no_dcqcn_or_pfc_words() {
    for name in EVALUATION {
        let image = lower(&format!("configs/benchmarks/evaluation/{name}"));
        for (backend, words) in measure(&image) {
            assert_eq!(
                words,
                MechanismPlaneWords {
                    pfc_region_words: 0,
                    dcqcn_receiver_rows: 0,
                    pfc_params_words: 1,
                    roce_region_words: 0,
                    roce_receiver_rows: 0,
                    pfc_class_words: Vec::new(),
                },
                "{backend} {name}"
            );
        }
    }
}

#[test]
fn p14_fixtures_plan_their_mechanism_state() {
    for (name, pfc, dcqcn) in [
        ("dcqcn_t26.toml", false, 1),
        ("dcqcn_t26_pfc.toml", true, 1),
        ("leanguard_pfc_executable.toml", true, 0),
        ("dcqcn_multi_zero_xoff.toml", true, 2),
    ] {
        let image = lower(&format!("configs/p14/{name}"));
        for (backend, words) in measure(&image) {
            assert_eq!(
                words.pfc_region_words != 0,
                pfc,
                "{backend} {name}: {words:?}"
            );
            assert_eq!(words.dcqcn_receiver_rows, dcqcn, "{backend} {name}");
        }
    }
}

/// P15 lane R4: queue-pair images plan the RoCE region (10 words per receiver since P16, whose
/// receiver holds no notification point; 13 in P15) and mark their
/// receiver rows; host-link PFC adds host rows; and the per-flow class word pins the feedback
/// class (ruling D2): `hostpfc_multi_qp_tcp`'s queue pairs carry data on class 3 and feedback on
/// class 0, so their word is `3 | (3 << 8)`, while its TCP flow (both classes 3) keeps `3`. P16 H4
/// (ruling G9): a queue pair at a host with a PFC row also carries its slot in that host's
/// queue-pair list in bits 16.. (the parked-bitset index): here slots 0, 1 and 2 at one host and
/// slot 0 at two others.
#[test]
fn p15_fixtures_plan_their_queue_pair_and_host_pfc_state() {
    for (name, pairs) in [
        ("roce_gbn_lossy.toml", 2),
        ("roce_lossless_pfc.toml", 2),
        ("hostpfc_multi_qp_tcp.toml", 5),
    ] {
        let image = lower(&format!("configs/p15/{name}"));
        for (backend, words) in measure(&image) {
            assert_eq!(words.roce_receiver_rows, pairs, "{backend} {name}");
            assert_eq!(words.roce_region_words, 10 * pairs, "{backend} {name}");
            assert_eq!(words.dcqcn_receiver_rows, 0, "{backend} {name}");
        }
    }
    let image = lower("configs/p15/hostpfc_multi_qp_tcp.toml");
    let slots = image
        .host_states
        .iter()
        .filter(|host| host.pfc.is_some())
        .flat_map(|host| {
            host.generators
                .iter()
                .filter(|generator| {
                    matches!(generator.kind, days_executor::FlowGeneratorKind::Roce(_))
                })
                .enumerate()
                .map(|(slot, generator)| (generator.flow, slot as u64))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        slots.values().copied().collect::<Vec<_>>(),
        [0, 1, 2, 0, 0],
        "five queue pairs at three host-PFC hosts"
    );
    let expected = image
        .flows
        .iter()
        .map(|flow| {
            let classes = match (flow.priority, flow.feedback_priority) {
                (3, 0) => 3 | (3 << 8),
                (3, 3) => 3,
                other => panic!("unexpected classes {other:?}"),
            };
            classes | slots.get(&flow.id).map_or(0, |slot| slot << 16)
        })
        .collect::<Vec<u64>>();
    assert!(expected.contains(&(3 | (3 << 8) | (2 << 16))) && expected.contains(&3));
    for (backend, words) in measure(&image) {
        assert_eq!(words.pfc_class_words, expected, "{backend}");
    }
}

/// P14 cuda-host: on an image without PFC state, one production CUDA plan walks the switch LPs for
/// PFC state once. Each scan walks every switch LP (49,152 nodes and 40,960 switch states on E6),
/// and the plan used to repeat it for the PFC region, the PFC frame bound and the PFC control
/// lanes, about 0.25 ms each (`evidence/P14/cuda-host.md` in days-gpu).
#[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
#[test]
fn a_plan_without_pfc_state_scans_the_fabric_for_it_once() {
    let image = lower("configs/benchmarks/evaluation/e6_cbr_k32_load_01.toml");
    let scans = days_executor::pfc_state_scans_cuda_plan_for_testing(
        &image,
        days_executor::CudaConfig::default(),
    )
    .expect("CUDA plan must size");
    assert_eq!(scans, 1, "PFC-state scans in one plain CUDA plan");
}
