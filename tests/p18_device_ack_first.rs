//! P18 ackfix on the device backends: control packets first at the host NIC (window-throttle
//! design F1, decision D16b) and the ACK-clocked restart of a window-parked pacer (F2),
//! byte-identical to the Scalar oracle.
//!
//! The images are the `configs/p18/` fixtures and checkpoints taken from them while a host queue
//! holds a control packet ahead of older data (restored packets lose their source instants, so the
//! device's restored keys must still put the control packet first), and a checkpoint with one data
//! packet moved ahead of its queue's control packets, a queue no F1 executor builds but validation
//! accepts, where a new ACK then arrives: every backend places a control packet after the leading
//! run of control packets whose key is not greater, counted from the head. Each runs to the stop
//! and to half the stop, under full observation (Scalar's diagnostics stripped: devices write no
//! transition records) and under summary observation.

#![cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, ObservationMode, PacketKind, RunResult, SimulationImage, run_scalar_with_observations,
    validate,
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

/// `image` with, in the first host queue that holds control packets ahead of data, the first data
/// packet moved to the head, ahead of the control packets, and that packet.
fn data_ahead_of_control(image: &SimulationImage) -> (SimulationImage, days_executor::PayloadId) {
    let mut moved = image.clone();
    let control = |payload| {
        image
            .initial_packets
            .iter()
            .find(|packet| packet.id == payload)
            .is_some_and(|packet| packet.kind.is_roce_control())
    };
    let state = moved
        .host_states
        .iter_mut()
        .find(|state| {
            state.queue.front().is_some_and(|payload| control(*payload))
                && state.queue.iter().any(|payload| !control(*payload))
        })
        .expect("a queue with control packets ahead of data");
    let data = state
        .queue
        .iter()
        .position(|payload| !control(*payload))
        .unwrap();
    let payload = state.queue.remove(data).unwrap();
    state.queue.push_front(payload);
    validate(&moved, Backend::Scalar).expect("validation accepts the reordered queue");
    (moved, payload)
}

/// `roce_ack_first` with 100 B packets from host 2 to host 0: host 0 sends an ACK every 0.8 us
/// while each of its own 1,000 B packets takes 8 us to send, so ACKs arrive while a data packet
/// waits at the head of its queue. The first checkpoint, on a 500 ns grid, whose queue (with one
/// data packet moved to its head) then takes a new ACK ahead of that data packet: the placement
/// counts the leading run of control packets from the head, which is empty there; a tail scan
/// would have put the ACK after the restored ones.
fn data_first_image() -> (String, SimulationImage) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p18/roce_ack_first.toml");
    let text = std::fs::read_to_string(&path).unwrap();
    let packet = "pkt_size_dist = { type = \"DiscreteUniform\", low = 1000, high = 1000 }";
    let second = text.rfind(packet).expect("the second flow's packet size");
    assert_ne!(text.find(packet), Some(second), "two flows");
    let small = format!(
        "{}pkt_size_dist = {{ type = \"DiscreteUniform\", low = 100, high = 100 }}{}",
        &text[..second],
        &text[second + packet.len()..]
    );
    let directory = tempfile::TempDir::new().unwrap();
    let small_path = directory.path().join("roce_ack_first_small_reverse.toml");
    std::fs::write(&small_path, small).unwrap();
    let image = compile_config(&small_path).expect("the variant lowers");
    let mut horizon = 500;
    while horizon < 2_000_000 {
        let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Summary)
            .expect("checkpoint prefix must run");
        if control_ahead_of_data(&prefix) {
            let (moved, data) = data_ahead_of_control(&checkpoint_image(&image, &prefix));
            let later = run_scalar_with_observations(
                &moved,
                Some(horizon + 3_000),
                ObservationMode::Summary,
            )
            .expect("the reordered checkpoint runs");
            let ahead = later.host_states.iter().any(|state| {
                let position = state.queue.iter().position(|payload| *payload == data);
                position.is_some_and(|position| {
                    position > 0
                        && later
                            .resident_packets
                            .iter()
                            .find(|packet| packet.id == state.queue[0])
                            .is_some_and(|packet| packet.kind.is_roce_control())
                })
            });
            if ahead {
                return (
                    format!("roce_ack_first_small_reverse@{horizon}-data-first"),
                    moved,
                );
            }
        }
        horizon += 500;
    }
    panic!("no reordered checkpoint takes an ACK ahead of its moved data packet");
}

/// The fixtures, then for each the first two checkpoints, on a 500 ns grid, whose host queue holds
/// a control packet ahead of older data, and the first of them with a data packet moved to the
/// head of that queue.
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
    images.push(data_first_image());
    images
}

#[test]
fn checkpoints_hold_control_packets_ahead_of_data() {
    let images = images();
    assert_eq!(images.len(), 3 * FIXTURES.len() + 1);
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
