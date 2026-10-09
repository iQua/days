//! P17 lane nocc: RoCE queue pairs without congestion control on the device backends,
//! byte-identical to the Scalar oracle (`days-gpu/evidence/P17/nocc/design.md` §3).
//!
//! The images are the `configs/p17/` fixtures and checkpoints taken from them while the pairs run
//! (pending timeouts, paced and parked pairs, CE echoes in flight). Each runs to the stop and to
//! half the stop, under full observation (Scalar's diagnostics stripped: devices write no
//! transition records) and under summary observation.

#![cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    FlowGeneratorKind, ObservationMode, RoceCongestionControl, RunResult, SimulationImage,
    run_scalar_with_observations,
};

const FIXTURES: &[&str] = &[
    "nocc_marked.toml",
    "nocc_unmarked.toml",
    "nocc_mixed.toml",
    "nocc_gbn_lossy.toml",
    "nocc_ring_lossless.toml",
];

fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p17")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
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

fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    let mut expected =
        run_scalar_with_observations(image, horizon, mode).expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

/// The fixtures, then checkpoints of the marked, mixed and lossy fixtures every 400 us up to 2 ms.
fn images() -> Vec<(String, SimulationImage)> {
    let mut images = FIXTURES
        .iter()
        .map(|name| (name.trim_end_matches(".toml").to_owned(), lower(name)))
        .collect::<Vec<_>>();
    for name in ["nocc_marked.toml", "nocc_mixed.toml", "nocc_gbn_lossy.toml"] {
        let image = lower(name);
        for horizon in (1..=5).map(|step| step * 400_000) {
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            images.push((
                format!("{}@{horizon}", name.trim_end_matches(".toml")),
                checkpoint_image(&image, &prefix),
            ));
        }
    }
    images
}

/// The images hold no-CC pairs, and some checkpoint holds one mid-run with a CE echo in flight.
#[test]
fn images_hold_nocc_pairs_and_echoes_in_flight() {
    let images = images();
    for (name, image) in &images {
        assert!(
            image
                .host_states
                .iter()
                .flat_map(|state| &state.generators)
                .any(
                    |generator| matches!(generator.kind, FlowGeneratorKind::Roce(roce)
                    if roce.congestion_control == RoceCongestionControl::None)
                ),
            "{name}: no pair without congestion control"
        );
    }
    let echo_in_flight = images.iter().any(|(_, image)| {
        image.initial_packets.iter().any(|packet| {
            matches!(packet.kind, days_executor::PacketKind::RoceAck(header) if header.ce_echo)
        })
    });
    assert!(
        echo_in_flight,
        "some checkpoint must carry a CE-echoing ACK"
    );
}

const MODES: [ObservationMode; 2] = [ObservationMode::Full, ObservationMode::Summary];

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, run_cuda_with_observations};

    use super::{MODES, images, scalar};

    #[test]
    fn cuda_nocc_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in MODES {
                    let expected = scalar(&image, horizon, mode);
                    for (streams_enabled, round_threads_per_block) in [(true, 256), (false, 32)] {
                        let actual = run_cuda_with_observations(
                            &image,
                            horizon,
                            CudaConfig {
                                streams_enabled,
                                round_threads_per_block,
                                ..CudaConfig::default()
                            },
                            mode,
                        )
                        .unwrap_or_else(|error| {
                            panic!("{name} horizon={horizon:?} {mode:?} streams={streams_enabled}: {error}")
                        });
                        assert!(
                            actual.result == expected,
                            "{name} horizon={horizon:?} {mode:?} streams={streams_enabled} \
                             threads={round_threads_per_block}: CUDA differs from Scalar"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, run_metal_with_observations};

    use super::{MODES, images, scalar};

    #[test]
    fn metal_nocc_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in MODES {
                    let expected = scalar(&image, horizon, mode);
                    for (streams_enabled, round_threads_per_threadgroup) in
                        [(true, 256), (false, 32)]
                    {
                        let actual = run_metal_with_observations(
                            &image,
                            horizon,
                            MetalConfig {
                                streams_enabled,
                                round_threads_per_threadgroup,
                                ..MetalConfig::default()
                            },
                            mode,
                        )
                        .unwrap_or_else(|error| {
                            panic!("{name} horizon={horizon:?} {mode:?} streams={streams_enabled}: {error}")
                        });
                        assert!(
                            actual.result == expected,
                            "{name} horizon={horizon:?} {mode:?} streams={streams_enabled} \
                             threads={round_threads_per_threadgroup}: Metal differs from Scalar"
                        );
                    }
                }
            }
        }
    }
}
