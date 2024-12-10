//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use csv::WriterBuilder;
use log::info;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use std::sync::{Arc, LazyLock, Mutex};

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
    // Shared state protected by locks
    shared_state: Arc<RwLock<SharedState>>,
    total_packets: Arc<AtomicUsize>,
}

impl CsvLogger {
    pub fn new() -> Self {
        CsvLogger {
            max_log_len: 10000,
            log_path: OnceLock::new(),
            report_interval: OnceLock::new(),
            shared_state: Arc::new(RwLock::new(SharedState::default())),
            total_packets: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn init(&self, config_path: Option<&str>, log_path: Option<&str>) {
        if let Some(config_path) = config_path {
            self.init_from_config(config_path);
        } else {
            self.init_default(log_path);
        }
    }

    fn ensure_trailing_slash(path: &str) -> String {
        if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        }
    }

    pub fn init_default(&self, log_path: Option<&str>) {
        let log_path = Self::ensure_trailing_slash(log_path.unwrap_or("./output"));

        self.log_path.set(log_path.clone()).unwrap();
        self.report_interval.set(f64::MAX).unwrap();

        self.init_output_files(&log_path);
    }

    pub fn init_from_config(&self, config_path: &str) {
        let content = fs::read_to_string(config_path).expect("The configuration is not valid");
        let log_config: LogConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of logging outputs");

        let log_path =
            Self::ensure_trailing_slash(&log_config.log_path.unwrap_or("./output".to_string()));

        self.log_path.set(log_path.clone()).unwrap();
        self.report_interval
            .set(log_config.report_interval.unwrap_or(f64::MAX))
            .unwrap();

        self.init_output_files(&log_path);
    }

    pub fn init_output_files(&self, log_path: &str) {
        if let Err(e) = fs::create_dir_all(log_path) {
            panic!(
                "Error '{}' occurred when creating directory {} for log files",
                e, log_path
            );
        };

        // Create output files
        for element in ["sources", "switches", "sinks"] {
            let file_name = format!("{}{}.csv", log_path, element);
            if let Err(e) = fs::File::create(&file_name) {
                panic!(
                    "Error '{}' occurred when creating log file {}",
                    e, &file_name
                );
            }
        }
    }

    pub fn get_instance() -> Arc<CsvLogger> {
        static INSTANCE: LazyLock<Mutex<Option<Arc<CsvLogger>>>> =
            LazyLock::new(|| Mutex::new(None));

        let mut instance = INSTANCE.lock().unwrap();
        if instance.is_none() {
            *instance = Some(Arc::new(CsvLogger::new()));
        }
        Arc::clone(instance.as_ref().unwrap())
    }

    pub fn get_report_interval(&self) -> f64 {
        *self.report_interval.get().unwrap()
    }

    pub fn log_report(report: Report, timing: ReportTiming) {
        let logger = &CsvLogger::get_instance();
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
        }

        // Release write lock before checking/flushing
        drop(state);

        if timing == ReportTiming::InProgress {
            logger.check_and_flush_reports();
        }
    }

    fn write_to_csv<T>(&self, element: ElementType, reports: &[T])
    where
        T: Serialize,
    {
        let csv_file_name = match element {
            ElementType::Source => format!("{}sources.csv", self.log_path.get().unwrap().clone()),
            ElementType::Scheduler => {
                format!("{}switches.csv", self.log_path.get().unwrap().clone())
            }
            ElementType::Sink => format!("{}sinks.csv", self.log_path.get().unwrap().clone()),
        };

        let csv_file = fs::OpenOptions::new()
            .append(true)
            .open(&csv_file_name)
            .unwrap();

        let write_header = csv_file.metadata().unwrap().len() == 0;

        let mut csv_writer = WriterBuilder::new()
            .has_headers(write_header)
            .from_writer(csv_file);

        for report in reports {
            if let Err(e) = csv_writer.serialize(report) {
                panic!(
                    "Error '{}' occurred when writing a report to csv file {}",
                    e, &csv_file_name
                );
            }
        }
    }

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

    fn check_and_flush_reports(&self) {
        // Get write lock to check and potentially flush reports
        let mut state = self.shared_state.write();

        // Check source reports
        if state.source_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.source_reports);
            self.write_to_csv(ElementType::Source, &reports);
        }

        // Check scheduler reports
        if state.scheduler_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.scheduler_reports);
            self.write_to_csv(ElementType::Scheduler, &reports);
        }

        // Check sink reports
        if state.sink_reports.len() >= self.max_log_len {
            let reports = std::mem::take(&mut state.sink_reports);
            self.write_to_csv(ElementType::Sink, &reports);

            let (new_packets, new_delay) = Self::compute_sink_statistics(&reports);
            self.total_packets.fetch_add(new_packets, Ordering::SeqCst);
            state.total_delay += new_delay;
        }
    }

    pub fn flush_reports() {
        let logger = &CsvLogger::get_instance();
        let mut state = logger.shared_state.write();

        // Write remaining reports
        if !state.source_reports.is_empty() {
            let reports = std::mem::take(&mut state.source_reports);
            logger.write_to_csv(ElementType::Source, &reports);
        }

        if !state.scheduler_reports.is_empty() {
            let reports = std::mem::take(&mut state.scheduler_reports);
            logger.write_to_csv(ElementType::Scheduler, &reports);
        }

        if !state.sink_reports.is_empty() {
            let reports = std::mem::take(&mut state.sink_reports);
            logger.write_to_csv(ElementType::Sink, &reports);

            let (final_packets, final_delay) = Self::compute_sink_statistics(&reports);
            logger
                .total_packets
                .fetch_add(final_packets, Ordering::SeqCst);
            state.total_delay += final_delay;
        }

        let total_packets = logger.total_packets.load(Ordering::SeqCst);
        let avg_delay = if total_packets > 0 {
            state.total_delay / total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);
    }
}
