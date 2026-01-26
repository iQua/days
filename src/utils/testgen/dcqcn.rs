use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::trace::open_csv;

const ALPHA_THRESHOLD_PPB: u64 = 100_000_000;

#[derive(Debug, Default, Clone)]
pub struct DcqcnTraceSummary {
    pub cnp_sent: usize,
    pub cnp_recv: usize,
    pub timer_tick: usize,
    pub cnp_sent_by_endpoint: HashMap<u64, usize>,
    pub cnp_recv_by_endpoint: HashMap<u64, usize>,
    pub timer_tick_by_endpoint: HashMap<u64, usize>,
    pub cnp_sent_by_flow: HashMap<u64, usize>,
    pub cnp_recv_by_flow: HashMap<u64, usize>,
    pub timer_tick_by_flow: HashMap<u64, usize>,
    pub cnp_apply: usize,
    pub cnp_ignored: usize,
    pub timer_with_cnp_seen: usize,
    pub timer_without_cnp_seen: usize,
    pub alpha_min_ppb: Option<u64>,
    pub alpha_max_ppb: Option<u64>,
    pub rate_min_bps: Option<u64>,
    pub rate_max_bps: Option<u64>,
    pub min_rate_bps: Option<u64>,
    pub max_rate_bps: Option<u64>,
    pub rate_clamped_min: bool,
    pub rate_clamped_max: bool,
}

impl DcqcnTraceSummary {
    pub fn coverpoints(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        if self.cnp_apply > 0 {
            out.insert("cnp_apply".to_string());
        }
        if self.cnp_ignored > 0 {
            out.insert("cnp_ignored_due_to_interval".to_string());
        }
        if self.timer_with_cnp_seen > 0 {
            out.insert("timer_with_cnp_seen".to_string());
        }
        if self.timer_without_cnp_seen > 0 {
            out.insert("timer_without_cnp_seen".to_string());
        }
        if self.alpha_max_ppb.is_some_and(|v| v >= ALPHA_THRESHOLD_PPB) {
            out.insert("alpha_above_0p1".to_string());
        }
        if self.alpha_min_ppb.is_some_and(|v| v < ALPHA_THRESHOLD_PPB) {
            out.insert("alpha_below_0p1".to_string());
        }
        if self.rate_clamped_min {
            out.insert("rate_clamped_min".to_string());
        }
        if self.rate_clamped_max {
            out.insert("rate_clamped_max".to_string());
        }
        out
    }
}

pub fn analyze_dcqcn_trace(path: &Path) -> Result<DcqcnTraceSummary, String> {
    let (mut reader, index) = open_csv(path)?;
    let mut summary = DcqcnTraceSummary::default();
    let mut cnp_seen_since_timer: HashMap<u64, bool> = HashMap::new();

    for result in reader.records() {
        let record = result.map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
        let kind = match index.get(&record, "kind") {
            Some(value) if !value.is_empty() => value.to_ascii_lowercase(),
            _ => continue,
        };

        let endpoint_id = index.get_u64(&record, "endpoint_id").unwrap_or(0);
        let flow_id = index.get_u64(&record, "flow_id").unwrap_or(0);
        let time_ns = index.get_u64(&record, "time_ns").unwrap_or(0);
        let cnp_interval_ns = index.get_u64(&record, "cnp_interval_ns").unwrap_or(0);
        let last_cnp_ns = index.get_u64(&record, "last_cnp_ns");

        if let Some(alpha_ppb) = index.get_u64(&record, "alpha_ppb") {
            summary.alpha_min_ppb = Some(
                summary
                    .alpha_min_ppb
                    .map_or(alpha_ppb, |v| v.min(alpha_ppb)),
            );
            summary.alpha_max_ppb = Some(
                summary
                    .alpha_max_ppb
                    .map_or(alpha_ppb, |v| v.max(alpha_ppb)),
            );
        }
        if let Some(rate_bps) = index.get_u64(&record, "rate_bps") {
            summary.rate_min_bps = Some(summary.rate_min_bps.map_or(rate_bps, |v| v.min(rate_bps)));
            summary.rate_max_bps = Some(summary.rate_max_bps.map_or(rate_bps, |v| v.max(rate_bps)));
            if let Some(min_rate) = index.get_u64(&record, "min_rate_bps") {
                summary.min_rate_bps = Some(min_rate);
                if rate_bps <= min_rate {
                    summary.rate_clamped_min = true;
                }
            }
            if let Some(max_rate) = index.get_u64(&record, "max_rate_bps") {
                summary.max_rate_bps = Some(max_rate);
                if rate_bps >= max_rate {
                    summary.rate_clamped_max = true;
                }
            }
        }

        match kind.as_str() {
            "cnp_sent" => {
                summary.cnp_sent += 1;
                *summary.cnp_sent_by_endpoint.entry(endpoint_id).or_insert(0) += 1;
                *summary.cnp_sent_by_flow.entry(flow_id).or_insert(0) += 1;
            }
            "cnp_recv" => {
                summary.cnp_recv += 1;
                *summary.cnp_recv_by_endpoint.entry(endpoint_id).or_insert(0) += 1;
                *summary.cnp_recv_by_flow.entry(flow_id).or_insert(0) += 1;
                let apply = match last_cnp_ns {
                    Some(last) if time_ns == last => true,
                    Some(last) => time_ns.saturating_sub(last) >= cnp_interval_ns,
                    None => true,
                };
                if apply {
                    summary.cnp_apply += 1;
                    cnp_seen_since_timer.insert(endpoint_id, true);
                } else {
                    summary.cnp_ignored += 1;
                }
            }
            "timer_tick" => {
                summary.timer_tick += 1;
                *summary
                    .timer_tick_by_endpoint
                    .entry(endpoint_id)
                    .or_insert(0) += 1;
                *summary.timer_tick_by_flow.entry(flow_id).or_insert(0) += 1;
                if cnp_seen_since_timer.remove(&endpoint_id).unwrap_or(false) {
                    summary.timer_with_cnp_seen += 1;
                } else {
                    summary.timer_without_cnp_seen += 1;
                }
            }
            _ => {}
        }
    }

    Ok(summary)
}
