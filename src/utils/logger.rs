//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use csv::WriterBuilder;
use log::info;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;
#[cfg(feature = "l2_pfc")]
use crate::l2::pfc::PfcPortReport;

#[derive(Deserialize)]
struct LogConfig {
    log_path: Option<String>,
    report_interval: Option<f64>,
}

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
    #[cfg(feature = "l2_pfc")]
    PfcPortReport(PfcPortReport),
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
    total_delay: f64,
}

/// Enum to represent the type of log element
enum ElementType {
    Source,
    Scheduler,
    Sink,
    #[cfg(feature = "l2_pfc")]
    Pfc,
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
        #[cfg(feature = "l2_pfc")]
        let elements = vec!["sources", "switches", "sinks", "pfc"];
        #[cfg(not(feature = "l2_pfc"))]
        let elements = vec!["sources", "switches", "sinks"];
        for element in elements {
            let file_name = format!("{}{}.csv", log_path, element);
            if let Err(e) = fs::File::create(&file_name) {
                return Err(format!("Error creating log file {}: {}", &file_name, e));
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

    /// Logs a report. Must be called after the logger has been initialized.
    pub fn log_report(report: Report, timing: ReportTiming) {
        let logger = CsvLogger::get_instance();
        if logger.log_path.get().is_none() {
            panic!("CsvLogger not initialized. Call init or init_from_config first.");
        }

        // Acquire the lock to modify shared state
        let mut state = logger.shared_state.write();

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
        }

        // Release the lock before potentially writing to disk
        drop(state);

        if timing == ReportTiming::InProgress {
            logger.check_and_flush_reports();
        }
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

        let total_packets = self.total_packets.load(Ordering::SeqCst);
        let avg_delay = if total_packets > 0 {
            state.total_delay / total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);
    }
}
