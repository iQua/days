//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use csv::WriterBuilder;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

// Import statements for PacketSinkReport, PacketSourceReport, SchedulerReport
use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;

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
    total_delay: f64,
}

/// Enum to represent the type of log element
enum ElementType {
    Source,
    Scheduler,
    Sink,
}

#[derive(Clone, Debug)]
pub struct CsvLogger {
    max_log_len: usize,
    log_path: OnceLock<String>,
    report_interval: OnceLock<f64>,
    // Shared state protected by a Mutex
    shared_state: Arc<Mutex<SharedState>>,
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
            shared_state: Arc::new(Mutex::new(SharedState::default())),
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
        if self.log_path.set(log_path.clone()).is_err() {
            return Err("Log path has already been set.".to_string());
        }

        // Set report_interval; default to f64::MAX if not set
        self.report_interval
            .set(f64::MAX)
            .map_err(|_| "Report interval already set.".to_string())?;

        self.init_output_files(&log_path)?;
        Ok(())
    }

    /// Initializes the CsvLogger from a configuration file.
    pub fn init_from_config(&self, config_path: &str) -> Result<(), String> {
        let content = fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;
        let log_config: LogConfig = toml::from_str(&content)
            .map_err(|e| format!("Failed to deserialize log configuration: {}", e))?;

        let log_path =
            Self::ensure_trailing_slash(&log_config.log_path.unwrap_or("./output".to_string()));

        // Attempt to set the log_path; return an error if already set
        if self.log_path.set(log_path.clone()).is_err() {
            return Err("Log path has already been set.".to_string());
        }

        // Set report_interval; default to f64::MAX if not set
        self.report_interval
            .set(log_config.report_interval.unwrap_or(f64::MAX))
            .map_err(|_| "Report interval already set.".to_string())?;

        self.init_output_files(&log_path)
    }

    /// Initializes the output CSV files.
    fn init_output_files(&self, log_path: &str) -> Result<(), String> {
        fs::create_dir_all(log_path)
            .map_err(|e| format!("Error creating log directory {}: {}", log_path, e))?;

        // Create output files
        for element in ["sources", "switches", "sinks"] {
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
    pub fn log_report(report: Report, timing: ReportTiming) -> Result<(), String> {
        let logger = CsvLogger::get_instance();
        if !logger.log_path.get().is_some() {
            return Err(
                "CsvLogger not initialized. Call init or init_from_config first.".to_string(),
            );
        }

        // Acquire the lock to modify shared state
        let mut state = logger
            .shared_state
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?;

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
        }

        // Release the lock before potentially writing to disk
        drop(state);

        if timing == ReportTiming::InProgress {
            logger.check_and_flush_reports();
        }

        Ok(())
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
        let mut state = match self.shared_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("Mutex poisoned. Recovering guarded data.");
                poisoned.into_inner()
            }
        };

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
    }

    /// Flushes all remaining reports to CSV files.
    pub fn flush_reports(&self) -> Result<(), String> {
        let mut state = self
            .shared_state
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?;

        // Write remaining source reports
        if !state.source_reports.is_empty() {
            let reports = std::mem::take(&mut state.source_reports);
            self.write_to_csv(ElementType::Source, &reports)?;
        }

        // Write remaining scheduler reports
        if !state.scheduler_reports.is_empty() {
            let reports = std::mem::take(&mut state.scheduler_reports);
            self.write_to_csv(ElementType::Scheduler, &reports)?;
        }

        // Write remaining sink reports
        if !state.sink_reports.is_empty() {
            let reports = std::mem::take(&mut state.sink_reports);
            self.write_to_csv(ElementType::Sink, &reports)?;

            let (final_packets, final_delay) = Self::compute_sink_statistics(&reports);
            self.total_packets
                .fetch_add(final_packets, Ordering::SeqCst);
            state.total_delay += final_delay;
        }

        let total_packets = self.total_packets.load(Ordering::SeqCst);
        let avg_delay = if total_packets > 0 {
            state.total_delay / total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_logger() {
        // Reset INSTANCE if possible (requires implementation)
        // CsvLogger::reset_instance();

        let logger = CsvLogger::get_instance();
        let log_path = "/test_logs/test_init_logger";

        // Initialize logger
        assert!(logger.init(log_path).is_ok());

        // Attempt to re-initialize should fail
        assert!(logger.init(log_path).is_err());
    }

    #[test]
    fn test_logging() {
        let logger = CsvLogger::get_instance();
        let log_path = "/test_logs/test_logging";

        // Initialize logger
        assert!(logger.init(log_path).is_ok());

        // Create sample reports
        let source_report = PacketSourceReport::default();
        let scheduler_report = SchedulerReport::default();
        let sink_report = PacketSinkReport::default();

        // Log reports
        assert!(CsvLogger::log_report(
            Report::PacketSourceReport(source_report),
            ReportTiming::InProgress
        )
        .is_ok());
        assert!(CsvLogger::log_report(
            Report::SchedulerReport(scheduler_report),
            ReportTiming::InProgress
        )
        .is_ok());
        assert!(CsvLogger::log_report(
            Report::PacketSinkReport(sink_report),
            ReportTiming::InProgress
        )
        .is_ok());

        // Flush reports
        assert!(logger.flush_reports().is_ok());

        // Verify CSV files exist
        assert!(fs::metadata(format!("{}/sources.csv", log_path)).is_ok());
        assert!(fs::metadata(format!("{}/switches.csv", log_path)).is_ok());
        assert!(fs::metadata(format!("{}/sinks.csv", log_path)).is_ok());
    }
}
