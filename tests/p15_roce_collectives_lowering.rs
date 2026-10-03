//! P15 lane R3: collective stages over RoCE queue pairs lower with their queue-pair keys, seeds
//! and gated state (`days-gpu/evidence/P15/collectives-design.md` §1, §2, §5.1, §6.1; rulings C1,
//! C3 and C5 of Oct 1, 2026).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    CollectivePhase, EventKind, FlowGeneratorKind, FlowGeneratorState, GeneratorStatus, PacketKind,
    SimulationImage, StageRole,
};

fn lower(name: &str) -> SimulationImage {
    compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
}

fn lower_text(label: &str, text: &str) -> Result<SimulationImage, String> {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p15-r3-{label}-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, text).expect("write the scenario");
    let result = compile_config(&path).map_err(|error| error.to_string());
    let _ = std::fs::remove_file(&path);
    result
}

fn fixture_text(name: &str) -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")),
    )
    .expect("read the fixture")
}

/// Every RoCE collective stage of an image: its generator, release flag and stage position.
fn roce_stages(
    image: &SimulationImage,
) -> Vec<(&FlowGeneratorState, bool, (CollectivePhase, u32, u32))> {
    image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter_map(|(generator, stage)| {
            let stage = stage?;
            let StageRole::Collective(identity) = stage.role else {
                return None;
            };
            matches!(generator.kind, FlowGeneratorKind::Roce(_)).then_some((
                generator,
                stage.activated,
                (identity.phase, identity.rank, identity.step),
            ))
        })
        .collect()
}

#[test]
fn roce_collectives_lower_one_queue_pair_per_stage_with_their_keys() {
    for (name, stages, rto_ns, feedback_priority) in [
        ("roce_ring_allreduce_lossless.toml", 24, 1_000_000, 0),
        ("roce_allgather_lossless.toml", 12, 0, 0),
        ("roce_ring_lossy.toml", 24, 1_000_000, 0),
        ("roce_compute_dag.toml", 24, 1_000_000, 0),
        ("roce_tcp_mixed_collectives.toml", 12, 1_000_000, 0),
    ] {
        let image = lower(name);
        let found = roce_stages(&image);
        assert_eq!(found.len(), stages, "{name}: RoCE stage count");
        for (generator, _, _) in &found {
            let FlowGeneratorKind::Roce(roce) = generator.kind else {
                unreachable!()
            };
            assert_eq!(roce.rto_ns, rto_ns, "{name}: the collective's RTO");
            assert_eq!(roce.pacer.mtu_bytes, 1000, "{name}: the collective's MTU");
            let flow = &image.flows[generator.flow.0 as usize];
            assert_eq!(flow.feedback_priority, feedback_priority, "{name}");
            // Every stage queue pair has its receiver at its target, with the key's cadence.
            let target = image
                .nodes
                .iter()
                .find(|node| node.id == flow.target)
                .expect("the target is a node");
            let receiver = image.host_states[target.state_slot as usize]
                .roce_receivers
                .as_deref()
                .and_then(|receivers| receivers.iter().find(|r| r.np.flow == flow.id))
                .unwrap_or_else(|| panic!("{name}: a stage queue pair has its receiver"));
            assert_eq!(receiver.total_bytes, roce.pacer.total_bytes);
            assert_eq!(receiver.ack_every_packets, 1);
            assert_eq!(receiver.ack_size_bytes, 64);
            assert_eq!(receiver.expected_psn, 0);
        }
    }
}

/// Ruling C1: both tokens of every RoCE stage are allocated at lowering; C5: a gated stage holds
/// its anchors at zero, a parked pacer, and no pending event; a root stage lowers exactly as a
/// plain queue pair (its first tick at the initial delay).
#[test]
fn gated_roce_stages_hold_their_tokens_zero_anchors_and_no_event() {
    for name in [
        "roce_ring_allreduce_lossless.toml",
        "roce_compute_dag.toml",
        "roce_allgather_lossless.toml",
    ] {
        let image = lower(name);
        let packets = image
            .initial_packets
            .iter()
            .map(|packet| (packet.id, *packet))
            .collect::<BTreeMap<_, _>>();
        let mut gated = 0;
        let mut roots = 0;
        for (generator, activated, _) in roce_stages(&image) {
            let FlowGeneratorKind::Roce(roce) = generator.kind else {
                unreachable!()
            };
            // The Mellanox-form controller owns no token (P16): one pacing token per stage.
            let packet = packets
                .get(&roce.pacing_timer_payload)
                .unwrap_or_else(|| panic!("{name}: a stage token is resident"));
            assert_eq!(
                (packet.kind, packet.flow, packet.size_bytes),
                (PacketKind::RocePacingTimer, generator.flow, 0)
            );
            let events = image
                .initial_events
                .iter()
                .filter(|event| event.payload == roce.pacing_timer_payload)
                .collect::<Vec<_>>();
            if activated {
                roots += 1;
                assert!(roce.pacer_armed, "{name}: a root's pacer is armed");
                assert_eq!(events.len(), 1, "{name}: a root has its pacing tick");
                assert!(
                    events
                        .iter()
                        .all(|event| event.kind == EventKind::PacingTimer)
                );
            } else {
                gated += 1;
                assert!(
                    events.is_empty(),
                    "{name}: a gated stage has no pending event"
                );
                assert!(!roce.pacer_armed);
                assert_eq!(generator.next_emission.status, GeneratorStatus::Blocked);
                assert_eq!(generator.next_emission.departure_time_ns, 0);
                assert_eq!(generator.next_emission.payload, roce.pacing_timer_payload);
                assert_eq!(roce.pacer.first_pacing_time_ns, 0, "{name}: anchor at zero");
                assert_eq!(
                    roce.controller,
                    days_executor::DcqcnController::pristine(roce.controller.config),
                    "{name}: a gated stage's controller is pristine"
                );
                assert_eq!(
                    (roce.next_psn, roce.snd_una, roce.rto_deadline_ns),
                    (0, 0, 0)
                );
                assert_eq!(roce.pacer.credit_quanta, 0);
            }
        }
        assert!(gated > 0, "{name}: some stage is gated");
        let expected_roots = if name == "roce_compute_dag.toml" {
            0
        } else {
            4
        };
        assert_eq!(roots, expected_roots, "{name}: released stages at lowering");
    }
}

#[test]
fn collective_transports_other_than_tcp_and_roce_stay_refused() {
    let base = fixture_text("roce_ring_lossy.toml");
    for (flow_type, expected) in [
        (
            "flow_type = \"DCQCN\"",
            "unsupported collective flow type `DCQCN`; collectives require a reliable transport, flow_type = \"TCP\" or \"RoCE\" (a RoCE queue pair is DCQCN with Go-back-N)",
        ),
        (
            "flow_type = \"PacketDistribution\"",
            "unsupported collective flow type `PacketDistribution`; collectives require a reliable transport, flow_type = \"TCP\" or \"RoCE\" (a RoCE queue pair is DCQCN with Go-back-N)",
        ),
        (
            "",
            "collective flow_type is missing; collectives require a reliable transport, flow_type = \"TCP\" or \"RoCE\"",
        ),
    ] {
        let text = base.replace("flow_type = \"RoCE\"", flow_type);
        let error = lower_text("transport", &text).expect_err("the transport is refused");
        assert_eq!(error, expected);
    }
}

#[test]
fn roce_collectives_need_a_nonempty_chunk_per_rank() {
    // AllGather: RingAllReduce refuses a size below the flow count before the transport check.
    let text = fixture_text("roce_allgather_lossless.toml").replace("size = 300000", "size = 3");
    assert_eq!(
        lower_text("chunk", &text).expect_err("an empty chunk is refused"),
        "invalid scenario: RoCE collective byte size 3 must be at least flow_count 4"
    );
}

/// Ruling C3: the retransmission timeout may be off on a RoCE collective, as on a plain pair.
#[test]
fn roce_collectives_accept_the_timeout_off() {
    let image = lower("roce_allgather_lossless.toml");
    assert!(roce_stages(&image).iter().all(|(generator, _, _)| matches!(
        generator.kind,
        FlowGeneratorKind::Roce(roce) if roce.rto_ns == 0
    )));
}

/// Design note §2: a stage queue pair's seed hashes its key's content, not its ordinal, so an
/// unrelated RoCE flow with a different key (which changes the ordinals) leaves it unchanged.
#[test]
fn roce_stage_seeds_hash_key_content_not_ordinals() {
    let base = fixture_text("roce_ring_lossy.toml");
    // A plain queue pair whose key sorts before the collective's (a smaller RTO).
    let extra = r#"
[[flow]]
flow_type = "RoCE"
priority = 0
graph = [[1, 0]]

[flow.traffic]
initial_delay = 0.0
size = 10000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }

[flow.traffic.dcqcn]
rate_gbps = 1.0
min_rate_gbps = 0.01
max_rate_gbps = 1.0
g = 0.00390625
ai_rate_gbps = 0.005
hai_rate_gbps = 0.05
rp_timer_ns = 50000
pacing_interval_ns = 1000

[flow.traffic.roce]
retransmit_timeout_ns = 500000
"#;
    let seeds = |image: &SimulationImage| {
        roce_stages(image)
            .into_iter()
            .map(|(generator, _, position)| (position, generator.rng_state))
            .collect::<BTreeMap<_, _>>()
    };
    let alone = lower_text("seed-alone", &base).expect("lowers");
    let with_flow = lower_text("seed-flow", &format!("{base}{extra}")).expect("lowers");
    assert_eq!(seeds(&alone).len(), 24);
    assert_eq!(seeds(&alone), seeds(&with_flow));

    // Flow identity depends only on content: with two plain pairs whose keys sort on either side
    // of the collective's, their parse order (and so their parse-order ordinals) does not matter.
    let slow = extra.replace(
        "retransmit_timeout_ns = 500000",
        "retransmit_timeout_ns = 2000000",
    );
    let one_order = lower_text("seed-ab", &format!("{base}{extra}{slow}")).expect("lowers");
    let other_order = lower_text("seed-ba", &format!("{base}{slow}{extra}")).expect("lowers");
    assert_eq!(format!("{one_order:#?}"), format!("{other_order:#?}"));
    assert_eq!(seeds(&alone), seeds(&one_order));
}

/// Design note §2: a TCP collective's stages keep their seeds next to a RoCE collective.
#[test]
fn tcp_stage_seeds_do_not_see_roce_collectives() {
    let mixed = lower("roce_tcp_mixed_collectives.toml");
    let text = fixture_text("roce_tcp_mixed_collectives.toml");
    let tcp_only = &text[..text.rfind("[[collective]]").expect("two collectives")];
    let alone = lower_text("tcp-alone", tcp_only).expect("lowers");
    let tcp_seeds = |image: &SimulationImage| {
        image
            .host_states
            .iter()
            .flat_map(|state| state.generators_with_stages())
            .filter_map(|(generator, stage)| match (generator.kind, stage?.role) {
                (FlowGeneratorKind::Tcp(_), StageRole::Collective(identity)) => Some((
                    (identity.phase, identity.rank, identity.step),
                    generator.rng_state,
                )),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(tcp_seeds(&alone).len(), 12);
    assert_eq!(tcp_seeds(&alone), tcp_seeds(&mixed));
}
