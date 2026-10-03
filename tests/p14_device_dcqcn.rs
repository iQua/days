//! P14 Lane B: DCQCN reaction/notification points on the device backends, byte-identical to the
//! Scalar oracle.
//!
//! The images are the `configs/p14/` DCQCN fixtures and checkpoints taken from them at many
//! horizons, so the devices resume from mid-run controller states: in-flight CNPs, Blocked pacing
//! tokens, pending control timers, every increase stage. Fixtures lowered with inert PFC state (all
//! XOFF thresholds zero) have that state stripped here; PFC itself is exercised by the PFC tests.

#![cfg(any(
    feature = "cuda",
    feature = "cuda-planner-test",
    all(feature = "metal", target_vendor = "apple")
))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    FlowGeneratorKind, GeneratorStatus, ObservationMode, PacketKind, RunResult, SimulationImage,
    run_scalar_with_observations,
};

fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p14")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

/// Removes PFC state whose every monitor is disabled (XOFF zero) and whose pause sets are empty,
/// together with the reverse PFC control channels, which lowering appends after every flow
/// channel. With no enabled priority the PFC paths are semantic no-ops, so the result is the same
/// DCQCN scenario without PFC state.
fn strip_inert_pfc(mut image: SimulationImage) -> SimulationImage {
    let mut control_channels = image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .filter_map(|queue| queue.pfc.as_ref())
        .flat_map(|pfc| &pfc.ingresses)
        .map(|ingress| ingress.control_channel_index as usize)
        .collect::<Vec<_>>();
    control_channels.sort_unstable();
    let first = image.channels.len() - control_channels.len();
    assert_eq!(
        control_channels,
        (first..image.channels.len()).collect::<Vec<_>>(),
        "PFC control channels must be the channel-table suffix"
    );
    image.channels.truncate(first);
    for queue in image
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
    {
        if let Some(pfc) = &queue.pfc {
            assert!(
                pfc.ingresses
                    .iter()
                    .all(|ingress| ingress.xoff_threshold_bytes == [0; 8])
                    && pfc.paused_by_controller.iter().all(|set| set.is_empty()),
                "only inert PFC state may be stripped"
            );
            queue.pfc = None;
        }
    }
    image
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

fn scalar(image: &SimulationImage, horizon: Option<u64>) -> RunResult {
    let mut expected = run_scalar_with_observations(image, horizon, ObservationMode::Full)
        .expect("scalar oracle must run");
    assert!(expected.diagnostics.is_some());
    expected.diagnostics = None;
    expected
}

/// The DCQCN images without PFC state: the T26 scenario, the CNP-heavy multi-hop fixture and the
/// 1 s fixture, plus checkpoints of the first two at every `step` ns.
fn dcqcn_images() -> Vec<(String, SimulationImage)> {
    let t26 = lower("dcqcn_t26.toml");
    let multi = strip_inert_pfc(lower("dcqcn_multi_zero_xoff.toml"));
    let one_second = strip_inert_pfc(lower("dcqcn_1s_zero_xoff.toml"));
    let mut images = vec![
        ("dcqcn_t26".to_owned(), t26.clone()),
        ("dcqcn_multi".to_owned(), multi.clone()),
        ("dcqcn_1s".to_owned(), one_second),
    ];
    for (name, image, step) in [
        ("dcqcn_t26", &t26, 25_000),
        ("dcqcn_multi", &multi, 9_000_000),
    ] {
        let mut horizon = step;
        while horizon < image.stop_time_ns {
            let prefix = run_scalar_with_observations(image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            images.push((
                format!("{name}@{horizon}"),
                checkpoint_image(image, &prefix),
            ));
            horizon += step;
        }
    }
    images
}

#[test]
fn dcqcn_fixtures_exercise_the_controller_and_carry_no_pfc_state() {
    for (name, image) in dcqcn_images() {
        assert!(
            image
                .switch_states
                .iter()
                .flat_map(|state| &state.queues)
                .all(|queue| queue.pfc.is_none()),
            "{name}"
        );
        days_executor::validate(&image, days_executor::Backend::Scalar)
            .unwrap_or_else(|error| panic!("{name} must validate: {error}"));
    }
    let multi = scalar(&strip_inert_pfc(lower("dcqcn_multi_zero_xoff.toml")), None);
    let cnps = multi
        .observed_packets
        .iter()
        .filter(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)))
        .count();
    assert_eq!(cnps, 195, "the multi-hop fixture must stay CNP-heavy");
}

/// A resumed checkpoint whose DCQCN source is Blocked with its controller armed (its timers are
/// lazy, P16: the pacing chain is the only live timer). `configs/p16/dcqcn_mlx_blocked.toml` paces
/// below one packet per tick and is still sending when its first cuts land; 1.7 ms is the first
/// 50 us horizon with such a source (scanned when the test was written; asserted here).
fn blocked_dcqcn_checkpoint() -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p16/dcqcn_mlx_blocked.toml");
    let image = compile_config(&path).expect("the blocked-source fixture lowers");
    let prefix = run_scalar_with_observations(&image, Some(1_700_000), ObservationMode::Full)
        .expect("checkpoint prefix must run");
    assert!(
        prefix
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .any(|generator| {
                matches!(generator.kind, FlowGeneratorKind::Dcqcn(dcqcn) if dcqcn.controller.armed)
                    && generator.next_emission.status == GeneratorStatus::Blocked
            }),
        "the checkpoint has a Blocked source with an armed controller"
    );
    checkpoint_image(&image, &prefix)
}

/// A resumed checkpoint of a lossy queue-pair fixture whose source LP holds two fallback-heap
/// records (the pacing tick and an armed retransmission timeout) while its Mellanox-form controller
/// is armed: a one-record heap must fault, retry, and reproduce the Scalar result, controller
/// state included. 9.92 ms is the latest 10 us horizon of `roce_gbn_lossy` with such a pair
/// (scanned when the test was written; asserted here), so the retried run replays little.
fn armed_queue_pair_checkpoint() -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p15/roce_gbn_lossy.toml");
    let image = compile_config(&path).expect("the lossy queue-pair fixture lowers");
    let prefix = run_scalar_with_observations(&image, Some(9_920_000), ObservationMode::Full)
        .expect("checkpoint prefix must run");
    assert!(
        prefix
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .any(|generator| {
                matches!(generator.kind, FlowGeneratorKind::Roce(roce)
                    if roce.controller.armed && roce.pacer_armed && roce.rto_deadline_ns != 0)
            }),
        "the checkpoint has an armed controller beside an armed timeout"
    );
    checkpoint_image(&image, &prefix)
}

#[test]
fn the_blocked_checkpoint_validates_and_owns_one_live_timer_chain() {
    let image = blocked_dcqcn_checkpoint();
    days_executor::validate(&image, days_executor::Backend::Scalar)
        .expect("the checkpoint must validate");
    let timers = image
        .initial_events
        .iter()
        .filter(|event| event.kind == days_executor::EventKind::PacingTimer)
        .count();
    let active_sources = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| {
            matches!(generator.kind, FlowGeneratorKind::Dcqcn(_))
                && matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                )
        })
        .count();
    assert_eq!(
        timers, active_sources,
        "each active source's pacing chain is its only live timer"
    );
}

/// The DCQCN planner terms (packet and CNP counts, control ticks, the second timer chain, the CNP
/// feedback minimum) agree between the precomputed and legacy capacity modes.
#[cfg(all(feature = "test", any(feature = "cuda", feature = "cuda-planner-test")))]
#[test]
fn cuda_dcqcn_planner_is_bit_equal_to_legacy_planning() {
    use days_executor::{CudaConfig, assert_cuda_planner_bit_equal_for_testing};
    let mut images = dcqcn_images();
    images.push(("blocked checkpoint".to_owned(), blocked_dcqcn_checkpoint()));
    for (name, image) in images {
        for streams_enabled in [true, false] {
            for observation_mode in [ObservationMode::Summary, ObservationMode::Full] {
                assert_cuda_planner_bit_equal_for_testing(
                    &image,
                    None,
                    CudaConfig {
                        streams_enabled,
                        ..CudaConfig::default()
                    },
                    observation_mode,
                )
                .unwrap_or_else(|error| {
                    panic!("{name} streams={streams_enabled} {observation_mode:?}: {error}")
                });
            }
        }
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaArena, CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{armed_queue_pair_checkpoint, dcqcn_images, scalar};

    #[test]
    fn cuda_dcqcn_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in dcqcn_images() {
            let heavy = name == "dcqcn_1s";
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                let expected = scalar(&image, horizon);
                let configs: &[(bool, usize)] = if heavy {
                    &[(true, 256)]
                } else {
                    &[(true, 256), (true, 32), (false, 32)]
                };
                for &(streams_enabled, round_threads_per_block) in configs {
                    let actual = run_cuda_with_observations(
                        &image,
                        horizon,
                        CudaConfig {
                            streams_enabled,
                            round_threads_per_block,
                            ..CudaConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "{name} horizon={horizon:?} streams={streams_enabled} \
                             threads={round_threads_per_block}: {error}"
                        )
                    });
                    assert_eq!(
                        actual.result, expected,
                        "{name} horizon={horizon:?} streams={streams_enabled} \
                         threads={round_threads_per_block}"
                    );
                }
            }
        }
    }

    /// The T24 gate failure: a resumed checkpoint with live timers hit a capacity fault the
    /// planner never sized for. Here the fallback heap is capped at one record while a queue pair
    /// holds two (P16: the controller has no timer chain of its own), so the first attempt must
    /// fault on the heap, retry, and still reproduce the Scalar result exactly, armed controller
    /// included.
    #[test]
    fn cuda_armed_controller_survives_a_fallback_heap_capacity_retry() {
        let image = armed_queue_pair_checkpoint();
        let expected = scalar(&image, None);
        let run = run_cuda_with_observations(
            &image,
            None,
            CudaConfig {
                max_fel_events_per_lp: Some(1),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("the retried run must succeed");
        assert!(
            run.capacity_retry_trace
                .iter()
                .any(|record| record.arena == CudaArena::Fel),
            "the one-record heap must fault and grow: {:?}",
            run.capacity_retry_trace
        );
        assert_eq!(run.result, expected);
    }
}

/// The Metal planner's DCQCN terms agree between the precomputed and legacy capacity modes.
#[cfg(all(feature = "test", feature = "metal", target_vendor = "apple"))]
#[test]
fn metal_dcqcn_planner_is_bit_equal_to_legacy_planning() {
    use days_executor::{MetalConfig, assert_metal_planner_bit_equal_for_testing};
    let mut images = dcqcn_images();
    images.push(("blocked checkpoint".to_owned(), blocked_dcqcn_checkpoint()));
    for (name, image) in images {
        for streams_enabled in [true, false] {
            for observation_mode in [ObservationMode::Summary, ObservationMode::Full] {
                assert_metal_planner_bit_equal_for_testing(
                    &image,
                    None,
                    MetalConfig {
                        streams_enabled,
                        ..MetalConfig::default()
                    },
                    observation_mode,
                )
                .unwrap_or_else(|error| {
                    panic!("{name} streams={streams_enabled} {observation_mode:?}: {error}")
                });
            }
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalArena, MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{armed_queue_pair_checkpoint, dcqcn_images, scalar};

    #[test]
    fn metal_dcqcn_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in dcqcn_images() {
            let heavy = name == "dcqcn_1s";
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                let expected = scalar(&image, horizon);
                let configs: &[(bool, usize)] = if heavy {
                    &[(true, 256)]
                } else {
                    &[(true, 256), (true, 32), (false, 32)]
                };
                for &(streams_enabled, round_threads_per_threadgroup) in configs {
                    let actual = run_metal_with_observations(
                        &image,
                        horizon,
                        MetalConfig {
                            streams_enabled,
                            round_threads_per_threadgroup,
                            ..MetalConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "{name} horizon={horizon:?} streams={streams_enabled} \
                             threads={round_threads_per_threadgroup}: {error}"
                        )
                    });
                    assert_eq!(
                        actual.result, expected,
                        "{name} horizon={horizon:?} streams={streams_enabled} \
                         threads={round_threads_per_threadgroup}"
                    );
                }
            }
        }
    }

    /// The Metal twin of the CUDA fallback-heap retry test.
    #[test]
    fn metal_armed_controller_survives_a_fallback_heap_capacity_retry() {
        let image = armed_queue_pair_checkpoint();
        let expected = scalar(&image, None);
        let run = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                max_fel_events_per_lp: Some(1),
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("the retried run must succeed");
        assert!(
            run.capacity_retry_trace
                .iter()
                .any(|record| record.arena == MetalArena::Fel),
            "the one-record heap must fault and grow: {:?}",
            run.capacity_retry_trace
        );
        assert_eq!(run.result, expected);
    }
}
