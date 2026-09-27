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
/// control-timer-heavy 1 s fixture, plus checkpoints of the first two at every `step` ns.
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

/// A resumed checkpoint whose DCQCN source is Blocked with both timer chains live.
fn blocked_dcqcn_checkpoint() -> SimulationImage {
    let image = lower("dcqcn_t26.toml");
    for horizon in (1_000..image.stop_time_ns).step_by(1_000) {
        let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
            .expect("checkpoint prefix must run");
        let generator = prefix.host_states[0].generators[0];
        let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
            panic!("the T26 source must stay DCQCN");
        };
        let control_live = prefix
            .pending_events
            .iter()
            .any(|event| event.payload == dcqcn.control_timer_payload);
        if generator.next_emission.status == GeneratorStatus::Blocked && control_live {
            return checkpoint_image(&image, &prefix);
        }
    }
    panic!("the T26 scenario must reach a Blocked source with a live control timer");
}

#[test]
fn the_blocked_checkpoint_validates_and_owns_two_live_timer_chains() {
    let image = blocked_dcqcn_checkpoint();
    days_executor::validate(&image, days_executor::Backend::Scalar)
        .expect("the checkpoint must validate");
    let timers = image
        .initial_events
        .iter()
        .filter(|event| event.kind == days_executor::EventKind::PacingTimer)
        .count();
    assert_eq!(timers, 2, "pacing and control chains must both be live");
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

    use super::{blocked_dcqcn_checkpoint, dcqcn_images, scalar};

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

    /// The T24 gate failure: a resumed checkpoint whose source is Blocked with a live timer hit a
    /// capacity fault the planner never sized for. Here the fallback heap is capped at one record
    /// while two DCQCN timer chains are live, so the first attempt must fault on the heap, retry,
    /// and still reproduce the Scalar result exactly.
    #[test]
    fn cuda_dcqcn_control_timer_survives_a_fallback_heap_capacity_retry() {
        let image = blocked_dcqcn_checkpoint();
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
