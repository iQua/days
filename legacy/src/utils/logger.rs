//! Legacy-safe facade over the shared CSV logger.

use std::sync::{Arc, OnceLock};

#[cfg(feature = "l2_pfc")]
pub use days::utils::logger::PfcPortReport;
#[cfg(feature = "lean")]
pub use days::utils::logger::{
    AqmEventKind, AqmEventRow, AqmLoggedEcnField, CubicEventKind, CubicEventRow, DrrEventKind,
    DrrEventRow, WfqEventKind, WfqEventRow,
};
pub use days::utils::logger::{
    CapacityUnit, DropAction, DropStrategyKind, PacketSinkReport, PacketSourceReport, Report,
    ReportTiming, SchedulerReport, TcpMetricsReport,
};
#[cfg(all(feature = "lean", feature = "dcqcn"))]
pub use days::utils::logger::{DcqcnEventKind, DcqcnEventRow, DcqcnLoggedEcnField};
#[cfg(all(feature = "lean", feature = "l2_pfc"))]
pub use days::utils::logger::{PfcEventKind, PfcEventRow};

use days::utils::logger::CsvLogger as SharedCsvLogger;

/// Shared logger with strict legacy validation on configuration-file initialization.
#[derive(Clone, Debug)]
pub struct CsvLogger {
    inner: Arc<SharedCsvLogger>,
}

impl Default for CsvLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvLogger {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SharedCsvLogger::new()),
        }
    }

    pub fn init(&self, log_path: &str) -> Result<(), String> {
        self.inner.init(log_path)
    }

    pub fn init_from_config(&self, config_path: &str) -> Result<(), String> {
        crate::validate_config(config_path)?;
        self.inner.init_from_config(config_path)
    }

    pub fn get_instance() -> Arc<Self> {
        static INSTANCE: OnceLock<Arc<CsvLogger>> = OnceLock::new();
        INSTANCE
            .get_or_init(|| {
                Arc::new(Self {
                    inner: SharedCsvLogger::get_instance(),
                })
            })
            .clone()
    }

    pub fn get_report_interval(&self) -> f64 {
        self.inner.get_report_interval()
    }

    #[cfg(all(feature = "lean", feature = "dcqcn"))]
    pub fn next_dcqcn_event_id() -> u64 {
        SharedCsvLogger::next_dcqcn_event_id()
    }

    #[cfg(feature = "lean")]
    pub fn next_cubic_event_id() -> u64 {
        SharedCsvLogger::next_cubic_event_id()
    }

    #[cfg(feature = "lean")]
    pub fn next_drr_event_id() -> u64 {
        SharedCsvLogger::next_drr_event_id()
    }

    #[cfg(feature = "lean")]
    pub fn next_wfq_event_id() -> u64 {
        SharedCsvLogger::next_wfq_event_id()
    }

    #[cfg(feature = "lean")]
    pub fn next_aqm_event_id() -> u64 {
        SharedCsvLogger::next_aqm_event_id()
    }

    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    pub fn next_pfc_event_id() -> u64 {
        SharedCsvLogger::next_pfc_event_id()
    }

    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    pub fn next_pfc_frame_id() -> u64 {
        SharedCsvLogger::next_pfc_frame_id()
    }

    pub fn log_report(report: Report, timing: ReportTiming) {
        SharedCsvLogger::log_report(report, timing);
    }

    pub fn try_log_report(report: Report, timing: ReportTiming) {
        SharedCsvLogger::try_log_report(report, timing);
    }

    #[cfg(feature = "test")]
    pub fn total_packets_sent(&self) -> usize {
        self.inner.total_packets_sent()
    }

    pub fn flush_reports(&self) {
        self.inner.flush_reports();
    }
}
