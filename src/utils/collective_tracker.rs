//! Minimal instrumentation to record collective completion time (CCT).
//!
//! This module keeps a small global mapping from flow IDs to a collective, and
//! emits one CSV row per collective when all terminal flows complete.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use crate::utils::logger::{CollectiveEventRow, CsvLogger, FlowEventRow, Report, ReportTiming};

#[derive(Debug)]
struct CollectiveState {
    collective_type: String,
    size_bytes: u64,
    start_time_s: Option<f64>,
    pending_terminal_flows: HashSet<usize>,
    end_time_s: f64,
    emitted: bool,
}

#[derive(Debug, Clone)]
struct FlowState {
    collective_id: usize,
    collective_type: String,
    size_bytes: u64,
    start_time_s: Option<f64>,
    end_time_s: f64,
    emitted: bool,
}

#[derive(Debug, Default)]
struct TrackerState {
    // flow_id -> collective_id mappings
    start_flows: HashMap<usize, usize>,
    terminal_flows: HashMap<usize, usize>,
    collectives: HashMap<usize, CollectiveState>,
    flows: HashMap<usize, FlowState>,
}

static TRACKER: OnceLock<Mutex<TrackerState>> = OnceLock::new();

fn state() -> &'static Mutex<TrackerState> {
    TRACKER.get_or_init(|| Mutex::new(TrackerState::default()))
}

pub struct CollectiveTracker;

impl CollectiveTracker {
    pub fn register_collective(
        collective_id: usize,
        collective_type: String,
        size_bytes: u64,
        start_flow_ids: Vec<usize>,
        terminal_flow_ids: Vec<usize>,
    ) {
        let mut guard = state().lock().expect("collective tracker lock poisoned");

        for flow_id in start_flow_ids {
            guard.start_flows.insert(flow_id, collective_id);
            guard.flows.entry(flow_id).or_insert_with(|| FlowState {
                collective_id,
                collective_type: collective_type.clone(),
                size_bytes,
                start_time_s: None,
                end_time_s: 0.0,
                emitted: false,
            });
        }
        for flow_id in terminal_flow_ids.iter().copied() {
            guard.terminal_flows.insert(flow_id, collective_id);
            guard.flows.entry(flow_id).or_insert_with(|| FlowState {
                collective_id,
                collective_type: collective_type.clone(),
                size_bytes,
                start_time_s: None,
                end_time_s: 0.0,
                emitted: false,
            });
        }

        guard.collectives.insert(
            collective_id,
            CollectiveState {
                collective_type,
                size_bytes,
                start_time_s: None,
                pending_terminal_flows: terminal_flow_ids.into_iter().collect(),
                end_time_s: 0.0,
                emitted: false,
            },
        );
    }

    pub fn on_flow_start(flow_id: usize, start_time_s: f64) {
        let mut guard = state().lock().expect("collective tracker lock poisoned");
        let Some(&collective_id) = guard.start_flows.get(&flow_id) else {
            return;
        };
        let Some(st) = guard.collectives.get_mut(&collective_id) else {
            return;
        };
        let new_t = start_time_s.max(0.0);
        st.start_time_s = Some(match st.start_time_s {
            Some(old) => old.min(new_t),
            None => new_t,
        });

        if let Some(flow) = guard.flows.get_mut(&flow_id) {
            flow.start_time_s = Some(match flow.start_time_s {
                Some(old) => old.min(new_t),
                None => new_t,
            });
        }
    }

    pub fn on_flow_end(flow_id: usize, end_time_s: f64) {
        let mut guard = state().lock().expect("collective tracker lock poisoned");
        let end_time_s = end_time_s.max(0.0);

        // Emit per-flow event (strict end timestamp), if this flow is tracked.
        if let Some(flow) = guard.flows.get_mut(&flow_id) {
            flow.end_time_s = flow.end_time_s.max(end_time_s);
            if !flow.emitted {
                if let Some(start) = flow.start_time_s {
                    let row = FlowEventRow {
                        flow_id: flow_id as u64,
                        collective_id: flow.collective_id as u64,
                        collective_type: flow.collective_type.clone(),
                        size_bytes: flow.size_bytes,
                        start_time_s: start,
                        end_time_s: flow.end_time_s,
                    };
                    CsvLogger::try_log_report(Report::FlowEventRow(row), ReportTiming::InProgress);
                    flow.emitted = true;
                }
            }
        }

        let Some(&collective_id) = guard.terminal_flows.get(&flow_id) else {
            return;
        };
        let Some(st) = guard.collectives.get_mut(&collective_id) else {
            return;
        };

        st.pending_terminal_flows.remove(&flow_id);
        st.end_time_s = st.end_time_s.max(end_time_s);

        if st.emitted || !st.pending_terminal_flows.is_empty() {
            return;
        }

        let Some(start_time_s) = st.start_time_s else {
            // If start wasn't observed yet, don't emit a bogus row.
            return;
        };

        let row = CollectiveEventRow {
            collective_id: collective_id as u64,
            collective_type: st.collective_type.clone(),
            size_bytes: st.size_bytes,
            start_time_s,
            end_time_s: st.end_time_s,
        };

        // logger might not be initialized in some unit tests; no-op in that case.
        CsvLogger::try_log_report(Report::CollectiveEventRow(row), ReportTiming::InProgress);
        st.emitted = true;
    }
}

