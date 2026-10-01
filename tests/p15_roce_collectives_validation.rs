//! P15 lane R3: the validator's rules for RoCE collective stages (design note §6.2, §6.3).
//!
//! Lowered images and Scalar checkpoints that hold gated, released and finished RoCE stages
//! validate; each one broken in one rule is refused with that rule's message.

use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    Backend, Event, EventKey, EventKind, FlowGeneratorKind, NodeKind, ObservationMode,
    SimulationImage, StageRole, run_scalar_with_observations, validate,
};

fn lower(name: &str) -> SimulationImage {
    compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
}

/// The state after a Scalar run up to `horizon_ns`, as a continuation image.
fn checkpoint(image: &SimulationImage, horizon_ns: u64) -> SimulationImage {
    let result = run_scalar_with_observations(image, Some(horizon_ns), ObservationMode::Full)
        .expect("the Scalar run to the horizon succeeds");
    SimulationImage {
        stop_time_ns: image.stop_time_ns,
        nodes: image.nodes.clone(),
        host_states: result.host_states,
        switch_states: result.switch_states,
        flows: image.flows.clone(),
        initial_packets: result.resident_packets,
        links: image.links.clone(),
        channels: image.channels.clone(),
        initial_events: result.pending_events,
        seed: image.seed,
    }
}

fn refused(image: &SimulationImage, needle: &str) {
    let error = validate(image, Backend::Scalar)
        .expect_err("the broken image must be refused")
        .to_string();
    assert!(error.contains(needle), "expected `{needle}` in: {error}");
}

/// (host slot, generator position) of every RoCE stage, with its release flag.
fn roce_stage_slots(image: &SimulationImage) -> Vec<(usize, usize, bool)> {
    image
        .host_states
        .iter()
        .enumerate()
        .flat_map(|(slot, state)| {
            state.generators_with_stages().enumerate().filter_map(
                move |(position, (generator, stage))| {
                    let stage = stage?;
                    (matches!(generator.kind, FlowGeneratorKind::Roce(_))
                        && matches!(stage.role, StageRole::Collective(_)))
                    .then_some((slot, position, stage.activated))
                },
            )
        })
        .collect()
}

fn with_roce(
    image: &SimulationImage,
    slot: usize,
    position: usize,
    edit: impl FnOnce(&mut days_executor::FlowGeneratorState, &mut days_executor::RoceGenerator),
) -> SimulationImage {
    let mut broken = image.clone();
    let generator = &mut broken.host_states[slot].generators[position];
    let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
        unreachable!()
    };
    edit(generator, &mut roce);
    generator.kind = FlowGeneratorKind::Roce(roce);
    broken
}

/// A gated stage holds the pristine state its release starts from (§6.1).
#[test]
fn a_gated_roce_stage_is_pristine_and_idle() {
    let image = lower("roce_compute_dag.toml");
    validate(&image, Backend::Scalar).expect("the lowered DAG validates");
    let (slot, position, activated) = roce_stage_slots(&image)[0];
    assert!(!activated, "every stage of the DAG is gated at lowering");

    let anchored = with_roce(&image, slot, position, |_, roce| {
        roce.pacer.first_pacing_time_ns = 1_000;
    });
    refused(
        &anchored,
        "dependency-blocked after its sending state changed",
    );
    let control = with_roce(&image, slot, position, |_, roce| {
        roce.controller.next_control_time_ns += 1_000;
    });
    refused(
        &control,
        "dependency-blocked after its sending state changed",
    );
    let credited = with_roce(&image, slot, position, |_, roce| {
        roce.pacer.credit_quanta = 1;
    });
    refused(
        &credited,
        "dependency-blocked after its sending state changed",
    );

    // A pending tick for a gated stage's token, at its scheduled departure.
    let mut ticking = image.clone();
    let host = &ticking.host_states[slot];
    let FlowGeneratorKind::Roce(roce) = host.generators[position].kind else {
        unreachable!()
    };
    let owner = ticking
        .nodes
        .iter()
        .find(|node| node.kind == NodeKind::Host && node.state_slot as usize == slot)
        .expect("the host's node")
        .id;
    ticking.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: 1,
            origin_node: owner,
            origin_seq: host.next_origin_seq,
        },
        target: owner,
        kind: EventKind::PacingTimer,
        payload: roce.pacing_timer_payload,
    });
    ticking.host_states[slot].next_origin_seq += 1;
    ticking.initial_events.sort_by_key(|event| event.key);
    refused(&ticking, "parked pacer owns a pending tick");
}

/// Gated RoCE stages reserve the payload identities of their future sends, as TCP stages do.
#[test]
fn gated_roce_stages_reserve_their_payload_identities() {
    let image = lower("roce_compute_dag.toml");
    let node_count = image.nodes.len() as u64;
    let mut exhausted = image;
    for node in exhausted
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let maximum_sequence = (u64::MAX - node.id.0) / node_count;
        exhausted.host_states[node.state_slot as usize].next_payload_seq = maximum_sequence + 1;
    }
    refused(&exhausted, "payload identity sequence");
}

/// A RoCE stage's inbound bytes are its receiver's Go-back-N frontier (§6.2 item 3).
#[test]
fn a_roce_stage_counts_its_receivers_frontier() {
    let image = lower("roce_ring_lossy.toml");
    let state = checkpoint(&image, 3_000_000);
    validate(&state, Backend::Scalar).expect("the checkpoint validates");
    // A gated stage whose inbound predecessor has delivered some bytes but not all.
    let (slot, position) = state
        .host_states
        .iter()
        .enumerate()
        .flat_map(|(slot, host)| {
            host.stages
                .iter()
                .enumerate()
                .filter_map(move |(position, stage)| {
                    let dependencies = stage.as_ref()?.dependencies;
                    (dependencies.inbound_bytes_received > 0
                        && !dependencies.inbound_predecessor_complete)
                        .then_some((slot, position))
                })
        })
        .next()
        .expect("a stage part-way through its inbound chunk at 3 ms");
    let mut broken = state.clone();
    let stage = broken.host_states[slot].stages[position]
        .as_mut()
        .expect("a stage");
    stage.dependencies.inbound_bytes_received -= 1;
    refused(&broken, "in-order RoCE frontier");
}

/// Host-link PFC (§5.3): a gated stage on a paused host is not on its parked list, and listing it
/// is refused; a checkpoint with one validates.
#[test]
fn gated_stages_on_a_paused_host_stay_off_its_parked_list() {
    let image = lower("roce_ring_allreduce_lossless.toml");
    let mut found = 0;
    for horizon_ns in (1..=60).map(|step| step * 50_000) {
        let state = checkpoint(&image, horizon_ns);
        validate(&state, Backend::Scalar)
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        for (slot, position, activated) in roce_stage_slots(&state) {
            let host = &state.host_states[slot];
            let Some(pfc) = host.pfc.as_deref() else {
                continue;
            };
            if activated || !pfc.is_paused(3) {
                continue;
            }
            found += 1;
            let mut listed = state.clone();
            listed.host_states[slot]
                .pfc
                .as_deref_mut()
                .expect("pause state")
                .pause_parked[3]
                .insert(position);
            refused(&listed, "parked list");
        }
    }
    assert!(
        found > 0,
        "no checkpoint held a gated stage on a paused host"
    );
}
