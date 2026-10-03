//! P16 D1 ruling D7: the queue-pair window (SimAI `HAS_WIN` and `VAR_WIN`; design note
//! `days-gpu/evidence/P16/dcqcn-design.md` §4).
//!
//! `[flow.traffic.roce] window_bytes` (default 0, no window) and `variable_window` (default
//! false). The window is `w = window_bytes`, or `max(1, floor(window_bytes * R_C / R_max))` with a
//! variable window, at the controller's rate as of the transition. A pacing tick that finds
//! `next_psn - snd_una >= w` sends nothing, adds no credit and parks (after the host-link PFC test);
//! only an ACK or NACK that moves `snd_una`, or a timeout, restarts it, never a host RESUME. An
//! armed pacer's status prediction is `Blocked` while the window binds.

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, DcqcnTransitionRecord, FlowGeneratorKind, FlowId, GeneratorStatus,
    MechanismTransitionRecord, NodeId, ObservationMode, RocePacerState, RoceSenderKind,
    RoceSenderRecord, RoceTransitionRecord, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_with_observations,
};
use tempfile::TempDir;

const WINDOW: &str = "configs/p16/dcqcn_mlx_window.toml";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn lower(relative: &str) -> SimulationImage {
    compile_config(repo_path(relative))
        .unwrap_or_else(|error| panic!("{relative} must lower: {error}"))
}

fn queue_pairs(image: &SimulationImage) -> Vec<(FlowId, days_executor::RoceGenerator)> {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => Some((generator.flow, roce)),
            _ => None,
        })
        .collect()
}

/// The window at controller rate `rate_bps` (design note §4), computed in `u128`.
fn window(record: &RoceSenderRecord, rate_bps: u64) -> u64 {
    if !record.variable_window {
        return record.window_bytes;
    }
    let scaled = u128::from(record.window_bytes) * u128::from(rate_bps)
        / u128::from(record.maximum_rate_bps);
    u64::try_from(scaled).unwrap().max(1)
}

#[test]
fn window_keys_lower_with_their_defaults() {
    let image = lower(WINDOW);
    let windows = queue_pairs(&image)
        .into_iter()
        .map(|(_, roce)| (roce.window_bytes, roce.variable_window, roce.window_parked))
        .collect::<Vec<_>>();
    assert_eq!(
        windows,
        [
            (200_000, false, false),
            (4_000, false, false),
            (20_000, true, false),
            (8_000, true, false)
        ]
    );
    for (_, roce) in queue_pairs(&lower("configs/p15/roce_gbn_lossy.toml")) {
        assert_eq!(
            (roce.window_bytes, roce.variable_window, roce.window_parked),
            (0, false, false),
            "no window by default"
        );
    }
}

#[test]
fn a_variable_window_needs_a_window() {
    let directory = TempDir::new().unwrap();
    let base = fs::read_to_string(repo_path(WINDOW)).unwrap();
    let path = directory.path().join("refused.toml");
    fs::write(
        &path,
        base.replacen(
            "window_bytes = 20000\nvariable_window = true\n",
            "variable_window = true\n",
            1,
        ),
    )
    .unwrap();
    let error = compile_config(&path).unwrap_err().to_string();
    assert!(
        error.contains("variable_window") && error.contains("window_bytes"),
        "{error}"
    );
    let path = directory.path().join("explicit_off.toml");
    fs::write(
        &path,
        base.replace("window_bytes = 200000\n", "window_bytes = 0\n")
            .replace("window_bytes = 20000\n", "window_bytes = 0\n")
            .replace("window_bytes = 8000\n", "window_bytes = 0\n")
            .replace("window_bytes = 4000\n", "window_bytes = 0\n")
            .replace("variable_window = true\n", "variable_window = false\n"),
    )
    .unwrap();
    for (_, roce) in queue_pairs(&compile_config(&path).expect("an explicit zero window lowers")) {
        assert_eq!((roce.window_bytes, roce.variable_window), (0, false));
    }
}

fn run_full(image: &SimulationImage) -> RunResult {
    run_scalar_with_observations(image, None, ObservationMode::Full).expect("the fixture runs")
}

/// Every sender record of the run with the controller rate as of it: the rate its own transition
/// leaves (`materialize(controller, bound)`, then feedback), from the pair's DCQCN records.
fn sender_rows_with_rates(
    image: &SimulationImage,
    result: &RunResult,
) -> Vec<(RoceSenderRecord, u64)> {
    let records = &result
        .diagnostics
        .as_ref()
        .expect("full")
        .mechanism_transitions;
    let mut controller_rows = BTreeMap::<FlowId, VecDeque<DcqcnTransitionRecord>>::new();
    for record in records {
        if let MechanismTransitionRecord::Dcqcn(row) = record {
            controller_rows.entry(row.flow).or_default().push_back(*row);
        }
    }
    let mut rates = queue_pairs(image)
        .into_iter()
        .map(|(flow, roce)| (flow, roce.controller.config.initial_rate_bps))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for record in records {
        let MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(row)) = record else {
            continue;
        };
        let rate = rates.get_mut(&row.flow).expect("a queue pair of the image");
        if let Some(queue) = controller_rows.get_mut(&row.flow) {
            while queue
                .front()
                .is_some_and(|controller| controller.key <= row.key)
            {
                *rate = queue.pop_front().unwrap().after.current_rate_bps;
            }
        }
        rows.push((*row, *rate));
    }
    rows
}

#[test]
fn window_blocked_ticks_park_without_credit_until_feedback() {
    let image = lower(WINDOW);
    let result = run_full(&image);
    let rows = sender_rows_with_rates(&image, &result);
    let mut blocked = BTreeMap::<FlowId, usize>::new();
    for (index, (row, rate)) in rows.iter().enumerate() {
        let outstanding = row.before.next_psn - row.before.snd_una;
        if row.window_blocked {
            *blocked.entry(row.flow).or_default() += 1;
            assert_eq!(row.kind, RoceSenderKind::Tick, "{row:?}");
            assert!(!row.class_paused && row.rate_bps.is_none() && row.emitted.is_none());
            assert!(outstanding >= window(row, *rate), "{row:?} at {rate}");
            assert_eq!(row.after.credit_quanta, row.before.credit_quanta);
            assert_eq!(row.after.pacer, RocePacerState::Parked);
            assert_eq!(row.after.status, GeneratorStatus::Blocked);
            // Only feedback or a timeout restarts a window-parked pacer: never a tick, a rate
            // change or a host RESUME.
            let next = rows[index + 1..]
                .iter()
                .find(|(later, _)| later.flow == row.flow)
                .expect("a window-parked pair hears from its receiver again");
            assert!(
                matches!(
                    next.0.kind,
                    RoceSenderKind::Ack | RoceSenderKind::Nack | RoceSenderKind::Timeout
                ),
                "{:?} after a window-blocked tick",
                next.0
            );
        } else if row.kind == RoceSenderKind::Tick && row.rate_bps.is_some() {
            assert!(
                row.window_bytes == 0 || outstanding < window(row, *rate),
                "a crediting tick inside a closed window: {row:?}"
            );
        }
        // An armed pacer predicts Blocked while its window binds.
        if row.after.pacer == RocePacerState::Armed
            && row.window_bytes != 0
            && row.after.next_psn < row.total_bytes
            && row.after.next_psn - row.after.snd_una >= window(row, *rate)
        {
            assert_eq!(row.after.status, GeneratorStatus::Blocked, "{row:?}");
        }
    }
    // The 4,000 B pair and both variable windows bind; the 200,000 B pair drives host 1's pauses.
    let small = rows
        .iter()
        .filter(|(row, _)| row.window_bytes <= 20_000)
        .map(|(row, _)| row.flow)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(small.len(), 3);
    assert!(
        small.iter().all(|flow| blocked.contains_key(flow)),
        "every small window binds: {blocked:?}"
    );
    let finished = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Finished)
        .count();
    assert_eq!(finished, 4, "every queue pair completes");
}

/// The instant after a host pause begins over a window-parked pair, and the RESUME that ends it
/// while the pair is still parked: the state the stored `window_parked` bit exists for (the
/// pause-parked list must not hold the pair, and the RESUME must not restart it).
fn paused_window_parked_interval(result: &RunResult) -> Option<(FlowId, u64, u64)> {
    let records = &result
        .diagnostics
        .as_ref()
        .expect("full")
        .mechanism_transitions;
    let mut parked_at = BTreeMap::<FlowId, (NodeId, u64)>::new();
    let mut asserted = BTreeMap::<(NodeId, u64), bool>::new();
    let mut pause_began = BTreeMap::<FlowId, u64>::new();
    for record in records {
        match record {
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(row)) => {
                if row.window_blocked {
                    parked_at.insert(row.flow, (row.node, row.key.time_ns));
                } else {
                    parked_at.remove(&row.flow);
                    pause_began.remove(&row.flow);
                }
            }
            MechanismTransitionRecord::PfcControl(control) if control.priority == 3 => {
                asserted.insert(
                    (control.node, control.controlled_link.0),
                    !control.after_controllers.is_empty(),
                );
                let paused = asserted
                    .iter()
                    .any(|(&(node, _), &on)| node == control.node && on);
                for (&flow, &(node, _)) in &parked_at {
                    if node != control.node {
                        continue;
                    }
                    if paused {
                        pause_began.entry(flow).or_insert(control.key.time_ns);
                    } else if let Some(&began) = pause_began.get(&flow) {
                        return Some((flow, began, control.key.time_ns));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

#[test]
fn a_host_resume_does_not_restart_a_window_parked_pair() {
    let image = lower(WINDOW);
    let full = run_full(&image);
    let (flow, paused_ns, resumed_ns) = paused_window_parked_interval(&full)
        .expect("some window-parked pair sits in a class paused at its host until a RESUME");
    // A checkpoint inside the pause holds the pair window-parked and outside the parked list;
    // resuming from it reproduces the uninterrupted run.
    let horizon = paused_ns + 1;
    assert!(horizon < resumed_ns);
    let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Summary)
        .expect("the prefix runs");
    let host = prefix
        .host_states
        .iter()
        .find(|state| {
            state
                .generators
                .iter()
                .any(|generator| generator.flow == flow)
        })
        .unwrap();
    let position = host
        .generators
        .iter()
        .position(|generator| generator.flow == flow)
        .unwrap();
    let FlowGeneratorKind::Roce(roce) = host.generators[position].kind else {
        unreachable!()
    };
    assert!(roce.window_parked && !roce.pacer_armed);
    let pfc = host.pfc.as_deref().expect("host-link PFC");
    assert!(pfc.is_paused(3) && !pfc.pause_parked[3].contains(&position));
    let mut resumed = image.clone();
    resumed.host_states.clone_from(&prefix.host_states);
    resumed.switch_states.clone_from(&prefix.switch_states);
    resumed.initial_packets.clone_from(&prefix.resident_packets);
    resumed.initial_events.clone_from(&prefix.pending_events);
    let finished = run_scalar_with_observations(&resumed, None, ObservationMode::Summary)
        .expect("the checkpoint validates and runs");
    assert_eq!(finished.host_states, full.host_states);
    assert_eq!(finished.switch_states, full.switch_states);
}

#[test]
fn the_window_fixture_matches_cpu_at_one_to_four_workers() {
    let image = lower(WINDOW);
    let scalar = run_full(&image);
    for workers in 1..=4 {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("CPU with {workers} workers: {error}"));
        assert!(cpu.result == scalar, "CPU with {workers} workers differs");
    }
}
