//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to CSV files.

use csv::WriterBuilder;
use log::info;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
#[cfg(feature = "lean")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
#[cfg(feature = "l2_pfc")]
use crate::l2::pfc::PfcPortReport;
use crate::schedulers::SchedulerReport;

#[cfg(feature = "lean")]
use crate::flows::packet::EcnField;
#[cfg(feature = "lean")]
use crate::schedulers::drop::{CapacityUnit, DropAction, DropStrategyKind};
use crate::utils::trace_manifest;

#[derive(Deserialize)]
struct LogConfig {
    log_path: Option<String>,
    report_interval: Option<f64>,
}

#[cfg(all(feature = "lean", feature = "l2_pfc"))]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PfcEventKind {
    PfcSent,
    PfcRecv,
}

#[cfg(all(feature = "lean", feature = "l2_pfc"))]
#[derive(Clone, Debug, Serialize)]
pub struct PfcEventRow {
    pub time_ns: u64,
    pub event_id: u64,
    pub kind: PfcEventKind,
    pub sender_id: u64,
    pub receiver_id: u64,
    pub priority: u8,
    pub pfc_frame_id: u64,
    pub class_enable: u8,
    pub pause_quanta: u16,
    pub queue_occupancy_bytes: Option<u64>,
    pub xoff_threshold_bytes: Option<u64>,
    pub xon_threshold_bytes: Option<u64>,
    pub buffer_capacity_bytes: Option<u64>,
    pub refresh_interval_ns: Option<u64>,
    pub drain_interval_ns: Option<u64>,
}

#[cfg(feature = "lean")]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CubicEventKind {
    Ack,
    Congestion,
    RecoveryExit,
    Timeout,
}

#[cfg(feature = "lean")]
#[derive(Clone, Debug, Serialize)]
pub struct CubicEventRow {
    pub time_ns: u64,
    pub event_id: u64,
    pub kind: CubicEventKind,
    pub endpoint_id: u64,
    pub flow_id: u64,
    pub acked_segs: Option<u64>,
    pub rtt_ns: Option<u64>,
    pub mss_bytes: u64,
    pub beta_ppb: u64,
    pub c_ppb: u64,
    pub tcp_friendly: bool,
    pub fast_convergence: bool,
    pub init_cwnd_bytes: u64,
    pub init_ssthresh_bytes: u64,
    pub flight_size_bytes: Option<u64>,
    pub cwnd_bytes: u64,
    pub ssthresh_bytes: u64,
    pub w_max_bytes: u64,
    pub w_last_max_bytes: u64,
    pub epoch_start_ns: Option<u64>,
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DcqcnEventKind {
    CnpSent,
    CnpRecv,
    TimerTick,
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
#[derive(Clone, Copy, Debug, Serialize)]
pub enum DcqcnLoggedEcnField {
    NotEct,
    Ect0,
    Ect1,
    Ce,
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
impl From<EcnField> for DcqcnLoggedEcnField {
    fn from(field: EcnField) -> Self {
        match field {
            EcnField::NotEct => DcqcnLoggedEcnField::NotEct,
            EcnField::Ect0 => DcqcnLoggedEcnField::Ect0,
            EcnField::Ect1 => DcqcnLoggedEcnField::Ect1,
            EcnField::Ce => DcqcnLoggedEcnField::Ce,
        }
    }
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
#[derive(Clone, Debug, Serialize)]
pub struct DcqcnEventRow {
    pub time_ns: u64,
    pub event_id: u64,
    pub kind: DcqcnEventKind,
    pub endpoint_id: u64,
    pub flow_id: u64,
    pub pkt_id: Option<u64>,
    pub pkt_flow_id: Option<u64>,
    pub trigger_ecn: Option<DcqcnLoggedEcnField>,
    pub cnp_priority: Option<u8>,
    pub cnp_size_b: Option<u64>,
    pub cnp_ecn: Option<DcqcnLoggedEcnField>,
    pub cnp_cwr: Option<bool>,
    pub cnp_last_packet: Option<bool>,
    pub cnp_interval_ns: u64,
    pub g_ppb: u64,
    pub mi_ppb: u64,
    pub init_rate_bps: u64,
    pub min_rate_bps: u64,
    pub max_rate_bps: u64,
    pub ai_rate_bps: u64,
    pub hai_rate_bps: u64,
    pub alpha_ppb: Option<u64>,
    pub rate_bps: Option<u64>,
    pub cnp_seen: Option<bool>,
    pub last_cnp_ns: Option<u64>,
}

#[cfg(feature = "lean")]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AqmEventKind {
    Decision,
}

#[cfg(feature = "lean")]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AqmLoggedEcnField {
    NotEct,
    Ect0,
    Ect1,
    Ce,
}

#[cfg(feature = "lean")]
impl From<EcnField> for AqmLoggedEcnField {
    fn from(field: EcnField) -> Self {
        match field {
            EcnField::NotEct => AqmLoggedEcnField::NotEct,
            EcnField::Ect0 => AqmLoggedEcnField::Ect0,
            EcnField::Ect1 => AqmLoggedEcnField::Ect1,
            EcnField::Ce => AqmLoggedEcnField::Ce,
        }
    }
}

#[cfg(feature = "lean")]
#[derive(Clone, Debug, Serialize)]
pub struct AqmEventRow {
    pub time_ns: u64,
    pub event_id: u64,
    pub kind: AqmEventKind,
    pub scheduler_id: u64,
    pub queue_id: u64,
    pub packet_id: u64,
    pub flow_id: u64,
    pub size_bytes: u64,
    pub action: DropAction,
    pub capacity: u64,
    pub capacity_unit: CapacityUnit,
    pub queue_length: u64,
    pub byte_length: u64,
    pub ecn_before: AqmLoggedEcnField,
    pub ecn_after: AqmLoggedEcnField,
    pub drop_strategy: DropStrategyKind,
    pub ecn_threshold_ppb: Option<u64>,
    pub red_min_threshold_ppb: Option<u64>,
    pub red_max_threshold_ppb: Option<u64>,
    pub red_max_probability_ppb: Option<u64>,
    pub red_avg_queue_length: Option<u64>,
    pub red_rand_max_ppb: Option<u64>,
    pub red_rand_min_ppb: Option<u64>,
}

#[cfg(feature = "lean")]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DrrEventKind {
    Enqueue,
    Schedule,
}

#[cfg(feature = "lean")]
#[derive(Clone, Debug, Serialize)]
pub struct DrrEventRow {
    pub time_ns: u64,
    pub event_id: u64,
    pub kind: DrrEventKind,
    pub scheduler_id: u64,
    pub class_count: u64,
    pub batch_id: Option<u64>,
    pub packet_id: u64,
    pub flow_id: u64,
    pub class_id: u64,
    pub size_bytes: u64,
    pub quantum_bytes: u64,
    pub deficit_bytes: u64,
    pub rate_bps: u64,
    pub current_queue: u64,
    pub scan_steps: u64,
    pub departure_time_ns: Option<u64>,
}

#[cfg(feature = "lean")]
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WfqEventKind {
    Enqueue,
    Schedule,
    Depart,
}

#[cfg(feature = "lean")]
#[derive(Clone, Debug, Serialize)]
pub struct WfqEventRow {
    pub time_ns: u64,
    pub event_id: u64,
    pub kind: WfqEventKind,
    pub scheduler_id: u64,
    pub packet_id: u64,
    pub flow_id: u64,
    pub class_id: u64,
    pub size_bytes: u64,
    pub weight: u64,
    pub rate_bps: u64,
    pub vtime_ns: u64,
    pub finish_time_ns: u64,
    pub departure_time_ns: Option<u64>,
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
static NEXT_DCQCN_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "lean")]
static NEXT_CUBIC_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "lean")]
static NEXT_DRR_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "lean")]
static NEXT_WFQ_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "lean")]
static NEXT_AQM_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(feature = "lean", feature = "l2_pfc"))]
static NEXT_PFC_EVENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(feature = "lean", feature = "l2_pfc"))]
static NEXT_PFC_FRAME_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
    #[cfg(feature = "l2_pfc")]
    PfcPortReport(PfcPortReport),
    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    PfcEventRow(PfcEventRow),
    #[cfg(feature = "lean")]
    CubicEventRow(CubicEventRow),
    #[cfg(feature = "lean")]
    DrrEventRow(DrrEventRow),
    #[cfg(feature = "lean")]
    WfqEventRow(WfqEventRow),
    #[cfg(all(feature = "lean", feature = "dcqcn"))]
    DcqcnEventRow(DcqcnEventRow),
    #[cfg(feature = "lean")]
    AqmEventRow(AqmEventRow),
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum ReportTiming {
    InProgress,
    Final,
}

// Shared state structure
#[derive(Default, Debug)]
struct SharedState {
    source_reports: Vec<PacketSourceReport>,
    scheduler_reports: Vec<SchedulerReport>,
    sink_reports: Vec<PacketSinkReport>,
    #[cfg(feature = "l2_pfc")]
    pfc_reports: Vec<PfcPortReport>,
    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    pfc_events: Vec<PfcEventRow>,
    #[cfg(feature = "lean")]
    cubic_events: Vec<CubicEventRow>,
    #[cfg(feature = "lean")]
    drr_events: Vec<DrrEventRow>,
    #[cfg(feature = "lean")]
    wfq_events: Vec<WfqEventRow>,
    #[cfg(all(feature = "lean", feature = "dcqcn"))]
    dcqcn_events: Vec<DcqcnEventRow>,
    #[cfg(feature = "lean")]
    aqm_events: Vec<AqmEventRow>,
    total_delay: f64,
}

/// Enum to represent the type of log element
enum ElementType {
    Source,
    Scheduler,
    Sink,
    #[cfg(feature = "l2_pfc")]
    Pfc,
    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    PfcEvents,
    #[cfg(feature = "lean")]
    CubicEvents,
    #[cfg(feature = "lean")]
    DrrEvents,
    #[cfg(feature = "lean")]
    WfqEvents,
    #[cfg(all(feature = "lean", feature = "dcqcn"))]
    DcqcnEvents,
    #[cfg(feature = "lean")]
    AqmEvents,
}

#[derive(Clone, Debug)]
pub struct CsvLogger {
    max_log_len: usize,
    log_path: OnceLock<String>,
    report_interval: OnceLock<f64>,
    // Shared state protected by locks
    shared_state: Arc<RwLock<SharedState>>,
    total_packets: Arc<AtomicUsize>,
}

impl Default for CsvLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvLogger {
    /// Creates a new CsvLogger instance with default settings.
    pub fn new() -> Self {
        CsvLogger {
            max_log_len: 10000,
            log_path: OnceLock::new(),
            report_interval: OnceLock::new(),
            shared_state: Arc::new(RwLock::new(SharedState::default())),
            total_packets: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Ensures that the provided path ends with a trailing slash.
    fn ensure_trailing_slash(path: &str) -> String {
        if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        }
    }

    /// Initializes the CsvLogger with a given log path.
    pub fn init(&self, log_path: &str) -> Result<(), String> {
        let log_path = Self::ensure_trailing_slash(log_path);

        // Attempt to set the log_path; return an error if already set
        self.log_path
            .set(log_path.clone())
            .expect("Log path has already been set.");

        // Setting the default report interval of f64::MAX
        self.report_interval
            .set(f64::MAX)
            .map_err(|_| "The report interval has already been set.".to_string())?;

        self.init_output_files(&log_path)
            .expect("Error initializing output files.");

        Ok(())
    }

    /// Initializes the CsvLogger from a configuration file.
    pub fn init_from_config(&self, config_path: &str) -> Result<(), String> {
        let content = fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;
        let log_config: LogConfig = toml::from_str(&content)
            .map_err(|e| format!("Failed to deserialize log configuration: {}", e))?;

        let log_path = Self::ensure_trailing_slash(
            &log_config
                .log_path
                .unwrap_or_else(|| "./output".to_string()),
        );

        self.log_path.set(log_path.clone())?;
        // Setting the report interval; default to f64::MAX if not set
        self.report_interval
            .set(log_config.report_interval.unwrap_or(f64::MAX))
            .map_err(|_| "The report interval has already been set.".to_string())?;

        self.init_output_files(&log_path)?;

        Ok(())
    }

    /// Initializes the output CSV files.
    fn init_output_files(&self, log_path: &str) -> Result<(), String> {
        fs::create_dir_all(log_path)
            .map_err(|e| format!("Error creating log directory {}: {}", log_path, e))?;

        // Create output files
        #[allow(unused_mut)]
        let mut elements = vec!["sources", "switches", "sinks"];
        #[cfg(feature = "l2_pfc")]
        elements.push("pfc");
        #[cfg(all(feature = "lean", feature = "l2_pfc"))]
        elements.push("pfc_events");
        #[cfg(feature = "lean")]
        elements.push("cubic_events");
        #[cfg(feature = "lean")]
        elements.push("drr_events");
        #[cfg(feature = "lean")]
        elements.push("wfq_events");
        #[cfg(feature = "lean")]
        elements.push("aqm_events");
        #[cfg(all(feature = "lean", feature = "dcqcn"))]
        elements.push("dcqcn_events");
        for element in elements {
            let file_name = format!("{}{}.csv", log_path, element);
            if let Err(e) = fs::File::create(&file_name) {
                return Err(format!("Error creating log file {}: {}", file_name, e));
            }
        }

        Ok(())
    }

    /// Retrieves the singleton instance of CsvLogger.
    pub fn get_instance() -> Arc<CsvLogger> {
        static INSTANCE: OnceLock<Arc<CsvLogger>> = OnceLock::new();
        INSTANCE.get_or_init(|| Arc::new(CsvLogger::new())).clone()
    }

    /// Retrieves the report interval.
    pub fn get_report_interval(&self) -> f64 {
        *self.report_interval.get().unwrap_or(&f64::MAX)
    }

    #[cfg(all(feature = "lean", feature = "dcqcn"))]
    pub fn next_dcqcn_event_id() -> u64 {
        NEXT_DCQCN_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(feature = "lean")]
    pub fn next_cubic_event_id() -> u64 {
        NEXT_CUBIC_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(feature = "lean")]
    pub fn next_drr_event_id() -> u64 {
        NEXT_DRR_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(feature = "lean")]
    pub fn next_wfq_event_id() -> u64 {
        NEXT_WFQ_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(feature = "lean")]
    pub fn next_aqm_event_id() -> u64 {
        NEXT_AQM_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    pub fn next_pfc_event_id() -> u64 {
        NEXT_PFC_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    pub fn next_pfc_frame_id() -> u64 {
        NEXT_PFC_FRAME_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn log_report_inner(&self, report: Report, timing: ReportTiming) {
        // Acquire the lock to modify shared state
        let mut state = self.shared_state.write();

        match report {
            Report::PacketSourceReport(report) => {
                state.source_reports.push(report);
            }
            Report::SchedulerReport(report) => {
                state.scheduler_reports.push(report);
            }
            Report::PacketSinkReport(report) => {
                state.sink_reports.push(report);
            }
            #[cfg(feature = "l2_pfc")]
            Report::PfcPortReport(report) => {
                state.pfc_reports.push(report);
            }
            #[cfg(all(feature = "lean", feature = "l2_pfc"))]
            Report::PfcEventRow(event) => {
                state.pfc_events.push(event);
            }
            #[cfg(feature = "lean")]
            Report::CubicEventRow(event) => {
                state.cubic_events.push(event);
            }
            #[cfg(feature = "lean")]
            Report::DrrEventRow(event) => {
                state.drr_events.push(event);
            }
            #[cfg(feature = "lean")]
            Report::WfqEventRow(event) => {
                state.wfq_events.push(event);
            }
            #[cfg(all(feature = "lean", feature = "dcqcn"))]
            Report::DcqcnEventRow(event) => {
                state.dcqcn_events.push(event);
            }
            #[cfg(feature = "lean")]
            Report::AqmEventRow(event) => {
                state.aqm_events.push(event);
            }
        }

        // Release the lock before potentially writing to disk.
        drop(state);

        if timing == ReportTiming::InProgress {
            self.check_and_flush_reports();
        }
    }

    /// Logs a report. Must be called after the logger has been initialized.
    pub fn log_report(report: Report, timing: ReportTiming) {
        let logger = CsvLogger::get_instance();
        if logger.log_path.get().is_none() {
            panic!("CsvLogger not initialized. Call init or init_from_config first.");
        }
        logger.log_report_inner(report, timing);
    }

    /// Logs a report if the logger has been initialized, otherwise no-ops.
    pub fn try_log_report(report: Report, timing: ReportTiming) {
        let logger = CsvLogger::get_instance();
        if logger.log_path.get().is_none() {
            return;
        }
        logger.log_report_inner(report, timing);
    }

    /// Writes reports to their respective CSV files.
    fn write_to_csv<T>(&self, element: ElementType, reports: &[T]) -> Result<(), String>
    where
        T: Serialize,
    {
        let csv_file_name = match element {
            ElementType::Source => format!("{}sources.csv", self.log_path.get().unwrap()),
            ElementType::Scheduler => format!("{}switches.csv", self.log_path.get().unwrap()),
            ElementType::Sink => format!("{}sinks.csv", self.log_path.get().unwrap()),
            #[cfg(feature = "l2_pfc")]
            ElementType::Pfc => format!("{}pfc.csv", self.log_path.get().unwrap()),
            #[cfg(all(feature = "lean", feature = "l2_pfc"))]
            ElementType::PfcEvents => format!("{}pfc_events.csv", self.log_path.get().unwrap()),
            #[cfg(feature = "lean")]
            ElementType::CubicEvents => format!("{}cubic_events.csv", self.log_path.get().unwrap()),
            #[cfg(feature = "lean")]
            ElementType::DrrEvents => format!("{}drr_events.csv", self.log_path.get().unwrap()),
            #[cfg(feature = "lean")]
            ElementType::WfqEvents => format!("{}wfq_events.csv", self.log_path.get().unwrap()),
            #[cfg(feature = "lean")]
            ElementType::AqmEvents => format!("{}aqm_events.csv", self.log_path.get().unwrap()),
            #[cfg(all(feature = "lean", feature = "dcqcn"))]
            ElementType::DcqcnEvents => {
                format!("{}dcqcn_events.csv", self.log_path.get().unwrap())
            }
        };

        let csv_file = fs::OpenOptions::new()
            .append(true)
            .open(&csv_file_name)
            .map_err(|e| format!("Failed to open {}: {}", csv_file_name, e))?;

        let need_header = csv_file
            .metadata()
            .map_err(|e| format!("Failed to get metadata for {}: {}", csv_file_name, e))?
            .len()
            == 0;

        let mut csv_writer = WriterBuilder::new()
            .has_headers(need_header)
            .from_writer(csv_file);

        for report in reports {
            csv_writer
                .serialize(report)
                .map_err(|e| format!("Failed to serialize report to {}: {}", csv_file_name, e))?;
        }

        csv_writer
            .flush()
            .map_err(|e| format!("Failed to flush CSV writer for {}: {}", csv_file_name, e))?;

        Ok(())
    }

    #[cfg(feature = "test")]
    /// Computes the total number of packets sent from source reports.
    pub fn total_packets_sent(&self) -> usize {
        let state = self.shared_state.read();

        let total_packets_sent = state
            .source_reports
            .iter()
            .map(|report| report.sent_packets)
            .sum::<usize>();

        total_packets_sent
    }

    /// Computes sink statistics from sink reports.
    fn compute_sink_statistics(reports: &[PacketSinkReport]) -> (usize, f64) {
        let total_packets = reports
            .iter()
            .map(|report| report.received_packets)
            .sum::<usize>();

        let total_delay = reports
            .iter()
            .map(|report| report.one_way_delay_mean * report.received_packets as f64)
            .sum::<f64>();

        (total_packets, total_delay)
    }

    /// Checks if reports exceed the maximum log length and flushes them if necessary.
    fn check_and_flush_reports(&self) {
        // Acquire the lock to modify shared state
        let mut state = self.shared_state.write();

        // Check and flush source reports
        if state.source_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.source_reports);
            if let Err(e) = self.write_to_csv(ElementType::Source, &reports) {
                eprintln!("Error writing source reports to CSV: {}", e);
            }
        }

        // Check and flush scheduler reports
        if state.scheduler_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.scheduler_reports);
            if let Err(e) = self.write_to_csv(ElementType::Scheduler, &reports) {
                eprintln!("Error writing scheduler reports to CSV: {}", e);
            }
        }

        // Check and flush sink reports
        if state.sink_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.sink_reports);
            if let Err(e) = self.write_to_csv(ElementType::Sink, &reports) {
                eprintln!("Error writing sink reports to CSV: {}", e);
            }

            let (new_packets, new_delay) = Self::compute_sink_statistics(&reports);
            self.total_packets.fetch_add(new_packets, Ordering::SeqCst);
            state.total_delay += new_delay;
        }

        #[cfg(feature = "l2_pfc")]
        if state.pfc_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.pfc_reports);
            if let Err(e) = self.write_to_csv(ElementType::Pfc, &reports) {
                eprintln!("Error writing PFC reports to CSV: {}", e);
            }
        }

        #[cfg(all(feature = "lean", feature = "l2_pfc"))]
        if state.pfc_events.len() >= self.max_log_len {
            let events = std::mem::take(&mut state.pfc_events);
            if let Err(e) = self.write_to_csv(ElementType::PfcEvents, &events) {
                eprintln!("Error writing PFC events to CSV: {}", e);
            }
        }

        #[cfg(feature = "lean")]
        if state.cubic_events.len() >= self.max_log_len {
            let events = std::mem::take(&mut state.cubic_events);
            if let Err(e) = self.write_to_csv(ElementType::CubicEvents, &events) {
                eprintln!("Error writing CUBIC events to CSV: {}", e);
            }
        }

        #[cfg(feature = "lean")]
        if state.drr_events.len() >= self.max_log_len {
            let events = std::mem::take(&mut state.drr_events);
            if let Err(e) = self.write_to_csv(ElementType::DrrEvents, &events) {
                eprintln!("Error writing DRR events to CSV: {}", e);
            }
        }

        #[cfg(feature = "lean")]
        if state.wfq_events.len() >= self.max_log_len {
            let events = std::mem::take(&mut state.wfq_events);
            if let Err(e) = self.write_to_csv(ElementType::WfqEvents, &events) {
                eprintln!("Error writing WFQ events to CSV: {}", e);
            }
        }

        #[cfg(feature = "lean")]
        if state.aqm_events.len() >= self.max_log_len {
            let events = std::mem::take(&mut state.aqm_events);
            if let Err(e) = self.write_to_csv(ElementType::AqmEvents, &events) {
                eprintln!("Error writing AQM events to CSV: {}", e);
            }
        }

        #[cfg(all(feature = "lean", feature = "dcqcn"))]
        if state.dcqcn_events.len() >= self.max_log_len {
            let events = std::mem::take(&mut state.dcqcn_events);
            if let Err(e) = self.write_to_csv(ElementType::DcqcnEvents, &events) {
                eprintln!("Error writing DCQCN events to CSV: {}", e);
            }
        }
    }

    /// Flushes all remaining reports to CSV files.
    pub fn flush_reports(&self) {
        let mut state = self.shared_state.write();

        // Write remaining source reports
        if !state.source_reports.is_empty() {
            let reports = std::mem::take(&mut state.source_reports);
            self.write_to_csv(ElementType::Source, &reports)
                .expect("Error writing source reports to CSV");
        }

        // Write remaining scheduler reports
        if !state.scheduler_reports.is_empty() {
            let reports = std::mem::take(&mut state.scheduler_reports);
            self.write_to_csv(ElementType::Scheduler, &reports)
                .expect("Error writing scheduler reports to CSV");
        }

        // Write remaining sink reports
        if !state.sink_reports.is_empty() {
            let reports = std::mem::take(&mut state.sink_reports);
            self.write_to_csv(ElementType::Sink, &reports)
                .expect("Error writing sink reports to CSV");

            let (final_packets, final_delay) = Self::compute_sink_statistics(&reports);
            self.total_packets
                .fetch_add(final_packets, Ordering::SeqCst);
            state.total_delay += final_delay;
        }

        #[cfg(feature = "l2_pfc")]
        if !state.pfc_reports.is_empty() {
            let reports = std::mem::take(&mut state.pfc_reports);
            self.write_to_csv(ElementType::Pfc, &reports)
                .expect("Error writing PFC reports to CSV");
        }

        #[cfg(all(feature = "lean", feature = "l2_pfc"))]
        if !state.pfc_events.is_empty() {
            let events = std::mem::take(&mut state.pfc_events);
            self.write_to_csv(ElementType::PfcEvents, &events)
                .expect("Error writing PFC events to CSV");
        }

        #[cfg(feature = "lean")]
        if !state.cubic_events.is_empty() {
            let events = std::mem::take(&mut state.cubic_events);
            self.write_to_csv(ElementType::CubicEvents, &events)
                .expect("Error writing CUBIC events to CSV");
        }

        #[cfg(feature = "lean")]
        if !state.drr_events.is_empty() {
            let events = std::mem::take(&mut state.drr_events);
            self.write_to_csv(ElementType::DrrEvents, &events)
                .expect("Error writing DRR events to CSV");
        }

        #[cfg(feature = "lean")]
        if !state.wfq_events.is_empty() {
            let events = std::mem::take(&mut state.wfq_events);
            self.write_to_csv(ElementType::WfqEvents, &events)
                .expect("Error writing WFQ events to CSV");
        }

        #[cfg(feature = "lean")]
        if !state.aqm_events.is_empty() {
            let events = std::mem::take(&mut state.aqm_events);
            self.write_to_csv(ElementType::AqmEvents, &events)
                .expect("Error writing AQM events to CSV");
        }

        #[cfg(all(feature = "lean", feature = "dcqcn"))]
        if !state.dcqcn_events.is_empty() {
            let events = std::mem::take(&mut state.dcqcn_events);
            self.write_to_csv(ElementType::DcqcnEvents, &events)
                .expect("Error writing DCQCN events to CSV");
        }

        let total_packets = self.total_packets.load(Ordering::SeqCst);
        let avg_delay = if total_packets > 0 {
            state.total_delay / total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);

        drop(state);

        if let Err(e) = self.write_trace_manifest_v1() {
            log::warn!("{e}");
        }
    }

    fn write_trace_manifest_v1(&self) -> Result<(), String> {
        let log_path = self
            .log_path
            .get()
            .ok_or_else(|| "CsvLogger not initialized (log_path missing)".to_string())?;

        let log_path = Path::new(log_path);
        let mut traces = Vec::new();

        for filename in Self::trace_manifest_candidates() {
            let path = log_path.join(filename);
            let len = fs::metadata(&path)
                .map_err(|e| format!("Failed to stat {}: {e}", path.display()))?
                .len();
            if len > 0 {
                traces.push(filename.to_string());
            }
        }

        trace_manifest::write_manifest_v1(log_path, traces)
    }

    fn trace_manifest_candidates() -> &'static [&'static str] {
        &[
            #[cfg(feature = "lean")]
            "aqm_events.csv",
            #[cfg(feature = "lean")]
            "cubic_events.csv",
            #[cfg(feature = "lean")]
            "drr_events.csv",
            #[cfg(feature = "lean")]
            "wfq_events.csv",
            #[cfg(all(feature = "lean", feature = "dcqcn"))]
            "dcqcn_events.csv",
            #[cfg(all(feature = "lean", feature = "l2_pfc"))]
            "pfc_events.csv",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sink_statistics_empty() {
        let (packets, delay) = CsvLogger::compute_sink_statistics(&[]);
        assert_eq!(packets, 0);
        assert_eq!(delay, 0.0);
    }

    #[test]
    fn test_compute_sink_statistics_weighted_delay() {
        let reports = vec![
            PacketSinkReport {
                received_packets: 2,
                one_way_delay_mean: 1.5,
                ..PacketSinkReport::default()
            },
            PacketSinkReport {
                received_packets: 3,
                one_way_delay_mean: 2.0,
                ..PacketSinkReport::default()
            },
        ];

        let (packets, delay) = CsvLogger::compute_sink_statistics(&reports);
        assert_eq!(packets, 5);
        assert!((delay - 9.0).abs() <= 1e-12);
    }
}
