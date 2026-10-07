//! P15 host-link PFC: the validator's rules for a host's egress pause state.
//!
//! A host owns `HostState::pfc` exactly when its egress link is PFC-controlled; its pause sets
//! name only the switch LPs that monitor that link; and its parked list holds exactly the queue
//! pairs a paused tick parked that can still restart (`evidence/P15/hostpfc-design.md` §3, §5, and
//! LeanGuard's writer contract, `leanguard.md` §12). Checkpoints of the lossless host-PFC incast,
//! taken mid-pause, revalidate on Scalar and CPU; the same checkpoints with one rule broken are
//! refused; and the device backends refuse host-link PFC until the device lane ports it.

use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    Backend, FlowGeneratorKind, HostPfcState, NodeId, ObservationMode, SimulationImage,
    run_scalar_with_observations, validate,
};

const FIXTURE: &str = "configs/p15/hostpfc_incast_lossless.toml";

fn lower(text_edit: Option<(&str, &str)>) -> SimulationImage {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let Some((from, to)) = text_edit else {
        return compile_config(&path).expect("the host-PFC fixture lowers");
    };
    let text = std::fs::read_to_string(&path).expect("read the host-PFC fixture");
    assert_eq!(text.matches(from).count(), 1, "{from:?}");
    let edited = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p15_hostpfc_validation_{}.toml",
        std::process::id()
    ));
    std::fs::write(&edited, text.replace(from, to)).expect("write the edited fixture");
    let image = compile_config(&edited).expect("the edited fixture lowers");
    let _ = std::fs::remove_file(&edited);
    image
}

/// The image's state after a Scalar run up to `horizon_ns`, as a continuation image.
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
        stage_joins: image.stage_joins.clone(),
        seeded_all_to_alls: image.seeded_all_to_alls.clone(),
    }
}

fn refused(image: &SimulationImage, needle: &str) {
    let error = validate(image, Backend::Scalar)
        .expect_err("the broken image must be refused")
        .to_string();
    assert!(error.contains(needle), "{error}");
}

fn host_pfc_mut(image: &mut SimulationImage, host: usize) -> &mut HostPfcState {
    image.host_states[host]
        .pfc
        .as_deref_mut()
        .expect("the host owns egress pause state")
}

/// P15 lane R4: every backend accepts host-link PFC (the devices hold host rows in the PFC region).
#[test]
fn the_lowered_image_validates_on_every_backend() {
    let image = lower(None);
    assert!(image.host_states.iter().all(|state| state.pfc.is_some()));
    validate(&image, Backend::Scalar).expect("Scalar accepts host-link PFC");
    validate(&image, Backend::Cpu { workers: 4 }).expect("CPU accepts host-link PFC");
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend)
            .unwrap_or_else(|error| panic!("{backend:?} accepts host-link PFC: {error}"));
    }
}

#[test]
fn a_host_owns_pause_state_exactly_when_its_egress_link_is_controlled() {
    let mut missing = lower(None);
    missing.host_states[1].pfc = None;
    refused(&missing, "must own host egress PFC state");

    let mut spurious = lower(Some(("\nhost_links = true\n", "\nhost_links = false\n")));
    validate(&spurious, Backend::Scalar).expect("switch-only PFC validates");
    spurious.host_states[1].pfc = Some(Box::default());
    refused(&spurious, "egress link is not PFC-controlled");
}

/// Checkpoints mid-run revalidate; the pause state and the parked list are pinned.
#[test]
fn mid_pause_checkpoints_revalidate_and_pin_the_parked_list() {
    let image = lower(None);
    let mut paused_checkpoints = 0;
    let mut parked_checkpoints = 0;
    for horizon_ns in (1..=40).map(|step| step * 250_000) {
        let state = checkpoint(&image, horizon_ns);
        validate(&state, Backend::Scalar)
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        validate(&state, Backend::Cpu { workers: 2 })
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        let Some((host, pfc)) = state
            .host_states
            .iter()
            .enumerate()
            .find_map(|(host, state)| {
                state
                    .pfc
                    .as_deref()
                    .filter(|pfc| (0..8).any(|class| pfc.is_paused(class)))
                    .map(|pfc| (host, pfc.clone()))
            })
        else {
            continue;
        };
        paused_checkpoints += 1;
        let class = (0..8).find(|class| pfc.is_paused(*class)).expect("paused");

        // A controller that does not monitor the host's link.
        let mut undeclared = state.clone();
        host_pfc_mut(&mut undeclared, host).paused_by_controller[class].insert(NodeId(u64::MAX));
        refused(&undeclared, "undeclared controller");

        if let Some(position) = pfc.pause_parked[class].first().copied() {
            parked_checkpoints += 1;
            // A parked queue pair dropped from the list: RESUME would never restart it.
            let mut dropped = state.clone();
            host_pfc_mut(&mut dropped, host).pause_parked[class].remove(&position);
            refused(&dropped, "parked list");
            // A parked queue pair listed under another class.
            let mut misfiled = state.clone();
            host_pfc_mut(&mut misfiled, host).pause_parked[class].remove(&position);
            host_pfc_mut(&mut misfiled, host).pause_parked[(class + 1) % 8].insert(position);
            refused(&misfiled, "parked list");
            // A listed queue pair whose pacer is armed.
            let mut armed = state.clone();
            if let FlowGeneratorKind::Roce(roce) =
                &mut armed.host_states[host].generators[position].kind
            {
                roce.pacer_armed = true;
            }
            assert!(validate(&armed, Backend::Scalar).is_err());
        }
    }
    assert!(paused_checkpoints > 0, "no checkpoint caught a host pause");
    assert!(
        parked_checkpoints > 0,
        "no checkpoint caught a parked queue pair"
    );
}

/// Fix round 1 of the host-PFC review (M2, R3): the parked list's upkeep after an ACK, NACK or
/// timeout restart. On the multi-pair, TCP-on-a-paused-class variant
/// (`configs/p15/hostpfc_multi_qp_tcp.toml`), checkpoints every 250 us over its first 10 ms
/// revalidate on Scalar and CPU (2 workers). At least one checkpoint lands inside a window between
/// a restart of a pause-parked pair (an `ack`, `nack` or `timeout` row taking the pacer from
/// `parked` to `armed`) and its re-park (that flow's next row, a `class_paused` tick): there a
/// stale parked-list entry would be refused, so the executor's upkeep is pinned, not only the
/// validator's rule.
#[test]
fn checkpoints_inside_a_restart_window_revalidate() {
    use days_executor::{
        MechanismTransitionRecord, RocePacerState, RoceSenderKind, RoceTransitionRecord,
    };
    let image = compile_config(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/p15/hostpfc_multi_qp_tcp.toml"),
    )
    .expect("the multi-pair variant lowers");
    let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the full run succeeds");
    let mut by_flow = std::collections::BTreeMap::<_, Vec<_>>::new();
    for record in &full
        .diagnostics
        .as_ref()
        .expect("full observation")
        .mechanism_transitions
    {
        if let MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender)) = record {
            by_flow.entry(sender.flow).or_default().push(*sender);
        }
    }
    // (restart time, re-park time) of every restart of a pause-parked pair.
    let windows = by_flow
        .values()
        .flat_map(|rows| rows.windows(2))
        .filter(|pair| {
            matches!(
                pair[0].kind,
                RoceSenderKind::Ack | RoceSenderKind::Nack | RoceSenderKind::Timeout
            ) && pair[0].before.pacer == RocePacerState::Parked
                && pair[0].after.pacer == RocePacerState::Armed
                && pair[1].kind == RoceSenderKind::Tick
                && pair[1].class_paused
        })
        .map(|pair| (pair[0].key.time_ns, pair[1].key.time_ns))
        .collect::<Vec<_>>();
    assert!(!windows.is_empty(), "no restart of a pause-parked pair");
    let mut inside = 0;
    for horizon_ns in (1..=40).map(|step| step * 250_000) {
        let state = checkpoint(&image, horizon_ns);
        validate(&state, Backend::Scalar)
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        validate(&state, Backend::Cpu { workers: 2 })
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        // The exclusive horizon has processed the restart and not yet the re-park.
        inside += usize::from(
            windows
                .iter()
                .any(|(restart, repark)| *restart < horizon_ns && horizon_ns <= *repark),
        );
    }
    assert!(
        inside > 0,
        "no checkpoint fell between a restart and its re-park ({} windows)",
        windows.len()
    );
}
