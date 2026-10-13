//! P18 ackfix on the device backends: control packets first at the host NIC (window-throttle
//! design F1, decision D16b) and the ACK-clocked restart of a window-parked pacer (F2),
//! byte-identical to the Scalar oracle.
//!
//! The images are the `configs/p18/` fixtures and checkpoints taken from them while a host queue
//! holds a control packet ahead of older data (restored packets lose their source instants, so the
//! device's restored keys must still put the control packet first). Each runs to the stop and to
//! half the stop, under full observation (Scalar's diagnostics stripped: devices write no
//! transition records) and under summary observation.

#![cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    ObservationMode, PacketKind, RunResult, SimulationImage, run_scalar_with_observations,
};

const FIXTURES: &[&str] = &["roce_ack_first.toml", "dcqcn_cnp_first.toml"];

fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p18")
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

/// Some host queue of the checkpoint holds a control packet ahead of a packet that is not one.
fn control_ahead_of_data(checkpoint: &RunResult) -> bool {
    let control = |payload| {
        checkpoint
            .resident_packets
            .iter()
            .find(|packet| packet.id == payload)
            .is_some_and(|packet| packet.kind.is_roce_control())
    };
    checkpoint.host_states.iter().any(|state| {
        let kinds = state
            .queue
            .iter()
            .map(|payload| control(*payload))
            .collect::<Vec<_>>();
        kinds
            .iter()
            .position(|is_control| *is_control)
            .is_some_and(|first| kinds[first..].iter().any(|is_control| !is_control))
    })
}

/// The fixtures, then for each the first two checkpoints, on a 500 ns grid, whose host queue holds
/// a control packet ahead of older data.
fn images() -> Vec<(String, SimulationImage)> {
    let mut images = Vec::new();
    for name in FIXTURES {
        let image = lower(name);
        let stem = name.trim_end_matches(".toml");
        images.push((stem.to_owned(), image.clone()));
        let mut found = 0;
        let mut horizon = 500;
        while found < 2 && horizon < image.stop_time_ns {
            let prefix =
                run_scalar_with_observations(&image, Some(horizon), ObservationMode::Summary)
                    .expect("checkpoint prefix must run");
            if control_ahead_of_data(&prefix) {
                images.push((
                    format!("{stem}@{horizon}"),
                    checkpoint_image(&image, &prefix),
                ));
                found += 1;
            }
            horizon += 500;
        }
        assert_eq!(
            found, 2,
            "{name}: a checkpoint holds a control packet ahead of data"
        );
    }
    images
}

#[test]
fn checkpoints_hold_control_packets_ahead_of_data() {
    let images = images();
    assert_eq!(images.len(), 3 * FIXTURES.len());
    assert!(images.iter().any(|(_, image)| {
        image
            .initial_packets
            .iter()
            .any(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)))
    }));
}

const MODES: [ObservationMode; 2] = [ObservationMode::Full, ObservationMode::Summary];

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, run_cuda_with_observations};

    use super::{MODES, images, scalar};

    #[test]
    fn cuda_ack_first_fixtures_and_checkpoints_match_scalar() {
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
    fn metal_ack_first_fixtures_and_checkpoints_match_scalar() {
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
