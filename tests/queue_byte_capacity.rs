//! Byte-unit switch-queue capacity: the exact packet-to-byte policy transform and the Metal
//! planner's single-shot sizing for it.
//!
//! These two tests were retained from the retired queue-byte ablation harness. The transform
//! rewrites every TailDrop packet capacity of a fixture into one exact byte capacity, so the
//! device planner sizes byte-unit queue arenas; the Metal test pins that the production plan for
//! the transformed K32 corpus completes without a capacity retry.

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{DropMarkPolicy, EcnRampPolicy, SimulationImage};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BytePolicyTransform {
    queues: usize,
    capacity_bytes: u64,
}

/// Replaces every TailDrop packet capacity with the equal byte capacity at `policy_packet_bytes`.
///
/// Every switch queue must start as TailDrop with the same nonzero packet capacity; anything else
/// is refused rather than approximated.
fn apply_probe_byte_policy(
    image: &mut SimulationImage,
    policy_packet_bytes: u64,
) -> Result<BytePolicyTransform, String> {
    if policy_packet_bytes == 0 {
        return Err("the policy packet size must be nonzero".to_owned());
    }

    let mut queues = 0_usize;
    let mut capacity_bytes = None;
    for (switch_slot, state) in image.switch_states.iter().enumerate() {
        for (queue_slot, queue) in state.queues.iter().enumerate() {
            if queue.drop_mark != DropMarkPolicy::TailDrop {
                return Err(format!(
                    "switch state {switch_slot} queue {queue_slot} must start as TailDrop"
                ));
            }
            let queue_capacity_bytes = queue
                .queue_capacity_packets
                .checked_mul(policy_packet_bytes)
                .ok_or_else(|| {
                    format!(
                        "switch state {switch_slot} queue {queue_slot} byte capacity overflows u64"
                    )
                })?;
            if queue_capacity_bytes == 0 {
                return Err(format!(
                    "switch state {switch_slot} queue {queue_slot} must have nonzero capacity"
                ));
            }
            if let Some(expected) = capacity_bytes {
                if queue_capacity_bytes != expected {
                    return Err(format!(
                        "switch state {switch_slot} queue {queue_slot} has byte capacity \
                         {queue_capacity_bytes}, expected {expected}"
                    ));
                }
            } else {
                capacity_bytes = Some(queue_capacity_bytes);
            }
            queues += 1;
        }
    }
    let capacity_bytes = capacity_bytes.ok_or_else(|| "fixture has no switch queues".to_owned())?;

    for queue in image
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
    {
        queue.drop_mark = DropMarkPolicy::EcnRamp(EcnRampPolicy {
            capacity_bytes,
            kmin_bytes: capacity_bytes,
            kmax_bytes: capacity_bytes,
            pmax_numerator: 1,
            pmax_denominator: 1,
        });
    }

    Ok(BytePolicyTransform {
        queues,
        capacity_bytes,
    })
}

#[test]
fn k16_probe_policy_is_derived_exactly() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/lookahead/rq9_closed_k16.toml");
    let mut image = compile_config(&path).expect("K16 fixture must lower");
    let transform = apply_probe_byte_policy(&mut image, 1_460).expect("transform must succeed");

    assert_eq!(transform.queues, 5_120);
    assert_eq!(transform.capacity_bytes, 93_440);
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        let DropMarkPolicy::EcnRamp(policy) = queue.drop_mark else {
            panic!("every switch queue must use the derived byte policy")
        };
        assert_eq!(policy.capacity_bytes, 93_440);
        assert_eq!(policy.kmin_bytes, 93_440);
        assert_eq!(policy.kmax_bytes, 93_440);
    }
}

#[cfg(all(feature = "test", feature = "metal", target_vendor = "apple"))]
#[test]
fn k32_byte_policy_strict_run_is_retry_free() {
    use days_executor::{DeviceCapacityCaps, MetalConfig, MetalExecutor, ObservationMode};

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/width_via_load_full/fattree_k32_load_90_sustained.toml");
    let mut image = compile_config(&path).expect("K32 byte-policy fixture must lower");
    apply_probe_byte_policy(&mut image, 256).expect("byte policy must derive exactly");
    let run = MetalExecutor::new()
        .expect("Metal executor must initialize")
        .run_with_observations(
            &image,
            None,
            MetalConfig {
                capacity_caps: DeviceCapacityCaps {
                    fallback_fel_events_per_lp: Some(16_384),
                    queue_packets_per_lp: Some(2_048),
                    channel_events_per_stream: Some(2_048),
                    remote_staging_events_per_lp: Some(2_048),
                    outbox_events_total: Some(2_000_000),
                    tcp_receiver_ranges_per_flow: Some(64),
                    tcp_ledger_segments_per_flow: Some(4_096),
                    observation_events_per_lp: Some(512),
                },
                max_capacity_retries: 0,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("strict K32 production plan must complete without replacement");

    assert!(
        run.capacity_retry_trace.is_empty(),
        "strict K32 production plan must not consume a retry"
    );
}
