//! P18 ackfix: the window-throttle design's F2 (`days-gpu/evidence/P18/window-throttle/design.md`
//! §3.2, user-approved Oct 13): when an ACK or NACK that moves `snd_una` finds the pacer parked by
//! the window (ruling D7), the pacer re-anchors its pacing grid at the feedback's instant and its
//! tick runs at that instant, after the feedback (a pacing timer is phase 1). Ruling D3 (the first
//! point of the old grid strictly after the instant) stays for timeouts and PFC RESUME restarts.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{
    Backend, CpuConfig, FlowGeneratorKind, FlowId, MechanismTransitionRecord, ObservationMode,
    RocePacerState, RoceSenderKind, RoceSenderRecord, RoceTransitionRecord, RunResult,
    SimulationImage, run_cpu_with_observations, run_scalar_with_observations, validate,
};
use tempfile::TempDir;

const WINDOW: &str = "configs/p16/dcqcn_mlx_window.toml";
const ACK_FIRST: &str = "configs/p18/roce_ack_first.toml";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn lower(relative: &str) -> SimulationImage {
    compile_config(repo_path(relative))
        .unwrap_or_else(|error| panic!("{relative} must lower: {error}"))
}

fn run_full(image: &SimulationImage) -> RunResult {
    run_scalar_with_observations(image, None, ObservationMode::Full).expect("the fixture runs")
}

fn sender_rows(result: &RunResult) -> Vec<RoceSenderRecord> {
    result
        .diagnostics
        .as_ref()
        .expect("full observation")
        .mechanism_transitions
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(row)) => Some(*row),
            _ => None,
        })
        .collect()
}

/// Each window-blocked tick with the pair's next sender row: the transition that ends the park.
fn window_park_ends(rows: &[RoceSenderRecord]) -> Vec<(RoceSenderRecord, RoceSenderRecord)> {
    let mut by_flow = BTreeMap::<FlowId, Vec<RoceSenderRecord>>::new();
    for row in rows {
        by_flow.entry(row.flow).or_default().push(*row);
    }
    let mut ends = Vec::new();
    for rows in by_flow.values() {
        for pair in rows.windows(2) {
            if pair[0].window_blocked {
                ends.push((pair[0], pair[1]));
            }
        }
    }
    ends
}

/// The first point of the pacing grid anchored at `anchor` strictly after `now` (ruling D3).
fn grid_point_after(anchor: u64, interval: u64, now: u64) -> u64 {
    if now < anchor {
        return anchor;
    }
    anchor + ((now - anchor) / interval + 1) * interval
}

/// The pacing tick that follows a restart: the pair's next tick row after `index`.
fn next_tick(rows: &[RoceSenderRecord], restart: &RoceSenderRecord) -> Option<RoceSenderRecord> {
    rows.iter()
        .filter(|row| row.flow == restart.flow && row.kind == RoceSenderKind::Tick)
        .find(|row| row.key > restart.key)
        .copied()
}

fn assert_feedback_restarts_at_its_instant(relative: &str) -> usize {
    let image = lower(relative);
    let result = run_full(&image);
    let rows = sender_rows(&result);
    let mut restarted = 0;
    for (park, end) in window_park_ends(&rows) {
        let feedback = matches!(end.kind, RoceSenderKind::Ack | RoceSenderKind::Nack);
        let moved = end.after.snd_una > end.before.snd_una
            || (end.kind == RoceSenderKind::Nack && end.after.next_psn < end.before.next_psn);
        if !(feedback && moved && end.after.pacer == RocePacerState::Armed) {
            continue;
        }
        let now = end.key.time_ns;
        assert_eq!(
            end.after.next_tick_ns,
            Some(now),
            "{relative}: the {:?} at {now} reopening the window of the pacer parked at {} \
             restarts it at its own instant (F2)",
            end.kind,
            park.key.time_ns
        );
        let tick = next_tick(&rows, &end).expect("the restarted tick runs");
        assert_eq!(tick.key.time_ns, now, "{relative}: {tick:?}");
        assert!(tick.key > end.key, "the tick runs after the feedback");
        assert_eq!(tick.before.next_tick_ns, Some(now));
        // The grid is re-anchored there: the tick after it lies one interval later.
        if tick.after.pacer == RocePacerState::Armed {
            assert_eq!(
                tick.after.next_tick_ns,
                Some(now + tick.pacing_interval_ns),
                "{relative}: {tick:?}"
            );
        }
        restarted += 1;
    }
    restarted
}

#[test]
fn a_window_parked_pacer_restarts_at_the_feedback_that_reopens_its_window() {
    let window = assert_feedback_restarts_at_its_instant(WINDOW);
    assert!(
        window > 0,
        "{WINDOW}: feedback restarts window-parked pacers"
    );
    let ack_first = assert_feedback_restarts_at_its_instant(ACK_FIRST);
    assert!(
        ack_first > 0,
        "{ACK_FIRST}: feedback restarts window-parked pacers"
    );
}

/// `configs/p15/roce_gbn_lossy.toml` with an 8,000 B window on each pair and a 100 us timeout:
/// tail losses leave some window-parked pair without feedback until its timeout fires.
fn lossy_window_image() -> SimulationImage {
    let directory = TempDir::new().unwrap();
    let text = fs::read_to_string(repo_path("configs/p15/roce_gbn_lossy.toml")).unwrap();
    let windowed = text.replace(
        "retransmit_timeout_ns = 1000000\n",
        "retransmit_timeout_ns = 100000\nwindow_bytes = 8000\n",
    );
    assert_ne!(windowed, text);
    let path = directory.path().join("roce_gbn_lossy_window.toml");
    fs::write(&path, windowed).unwrap();
    compile_config(&path).expect("the windowed lossy variant lowers")
}

/// Ruling D3 stays for a timeout: a timeout that restarts a window-parked pacer restarts it on the
/// first point of its old grid strictly after the timeout, and the grid keeps its anchor.
#[test]
fn a_timeout_restarts_a_window_parked_pacer_on_its_old_grid() {
    let image = lossy_window_image();
    let result = run_full(&image);
    let rows = sender_rows(&result);
    let mut anchors = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => {
                Some((generator.flow, roce.pacer.first_pacing_time_ns))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut timeouts = 0;
    let mut feedback = 0;
    for (index, row) in rows.iter().enumerate() {
        let parked_by_window = rows[..index]
            .iter()
            .rev()
            .find(|earlier| earlier.flow == row.flow)
            .is_some_and(|earlier| earlier.window_blocked);
        let anchor = anchors[&row.flow];
        if parked_by_window && row.after.pacer == RocePacerState::Armed {
            match row.kind {
                RoceSenderKind::Timeout => {
                    assert_eq!(
                        row.after.next_tick_ns,
                        Some(grid_point_after(
                            anchor,
                            row.pacing_interval_ns,
                            row.key.time_ns
                        )),
                        "{row:?}"
                    );
                    timeouts += 1;
                }
                RoceSenderKind::Ack | RoceSenderKind::Nack => {
                    *anchors.get_mut(&row.flow).unwrap() = row.key.time_ns;
                    feedback += 1;
                }
                _ => {}
            }
        }
    }
    assert!(timeouts > 0, "a timeout restarts a window-parked pacer");
    assert!(feedback > 0, "feedback restarts window-parked pacers");
    for workers in [1, 3] {
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
        assert!(cpu.result == result, "CPU with {workers} workers differs");
    }
}

/// A checkpoint taken after feedback re-anchored a pair's grid holds the moved anchor: it
/// validates, and the resumed run equals the uninterrupted one on Scalar and CPU.
#[test]
fn a_checkpoint_after_a_reanchored_restart_resumes_exactly() {
    for relative in [WINDOW, ACK_FIRST] {
        let image = lower(relative);
        let full = run_full(&image);
        let rows = sender_rows(&full);
        let (_, end) = window_park_ends(&rows)
            .into_iter()
            .filter(|(_, end)| {
                matches!(end.kind, RoceSenderKind::Ack | RoceSenderKind::Nack)
                    && end.after.pacer == RocePacerState::Armed
            })
            .min_by_key(|(_, end)| end.key)
            .expect("a feedback restart");
        // Between the restart and its tick nothing else runs at the pair; the checkpoint lands
        // after both, one nanosecond past the restart instant.
        let horizon = end.key.time_ns + 1;
        let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Summary)
            .expect("the prefix runs");
        let moved = prefix
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .find(|generator| generator.flow == end.flow)
            .map(|generator| match generator.kind {
                FlowGeneratorKind::Roce(roce) => roce.pacer.first_pacing_time_ns,
                _ => unreachable!(),
            })
            .unwrap();
        assert_eq!(
            moved, end.key.time_ns,
            "{relative}: the anchor moved to the restart"
        );
        let mut resumed = image.clone();
        resumed.host_states.clone_from(&prefix.host_states);
        resumed.switch_states.clone_from(&prefix.switch_states);
        resumed.initial_packets.clone_from(&prefix.resident_packets);
        resumed.initial_events.clone_from(&prefix.pending_events);
        validate(&resumed, Backend::Scalar).expect("the checkpoint validates");
        let uninterrupted =
            run_scalar_with_observations(&image, None, ObservationMode::Summary).unwrap();
        let finished = run_scalar_with_observations(&resumed, None, ObservationMode::Summary)
            .expect("the checkpoint runs");
        assert_eq!(
            finished.host_states, uninterrupted.host_states,
            "{relative}"
        );
        assert_eq!(
            finished.switch_states, uninterrupted.switch_states,
            "{relative}"
        );
        assert_eq!(
            finished.pending_events, uninterrupted.pending_events,
            "{relative}"
        );
        for workers in [1, 2, 4] {
            let cpu = run_cpu_with_observations(
                &resumed,
                None,
                CpuConfig {
                    workers,
                    ..CpuConfig::default()
                },
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("CPU with {workers} workers: {error}"));
            assert!(
                cpu.result == finished,
                "{relative}: CPU with {workers} workers differs"
            );
        }
    }
}
