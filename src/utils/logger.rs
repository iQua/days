//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use parking_lot::RwLock;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use csv::WriterBuilder;
use log::info;
use serde::{Deserialize, Serialize};

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::get_config_path;
use crate::schedulers::SchedulerReport;
use crate::utils::ui::{Report, ReportTiming};

#[derive(Deserialize)]
struct LogConfig {
    log_path: Option<String>,
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
    log_dir: Arc<String>,
    max_log_len: usize,
    // Shared state protected by locks
    shared_state: Arc<RwLock<SharedState>>,
    total_packets: Arc<AtomicUsize>,
    file_lock: Arc<Mutex<()>>, // Lock for file operations
}

impl CsvLogger {
    pub fn new() -> Self {
        let file_path = get_config_path();
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");
        let log_config: LogConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of logging outputs");

        let mut log_dir = log_config.log_path.unwrap_or("./output/".to_string());
        if log_dir.chars().last().unwrap() != '/' {
            log_dir.push('/');
        }

        if let Err(e) = fs::create_dir_all(&log_dir) {
            panic!(
                "Error '{}' occurred when creating directory {} for log files",
                e, &log_dir
            );
        };

        // Create output files
        for element in ["sources", "switches", "sinks"] {
            let file_name = format!("{}{}.csv", log_dir, element);
            if let Err(e) = fs::File::create(&file_name) {
                panic!(
                    "Error '{}' occurred when creating log file {}",
                    e, &file_name
                );
            }
        }

        CsvLogger {
            log_dir: Arc::new(log_dir),
            max_log_len: 10000,
            shared_state: Arc::new(RwLock::new(SharedState::default())),
            total_packets: Arc::new(AtomicUsize::new(0)),
            file_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn log_report(&self, report: Report, timing: ReportTiming) {
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
        }

        // Release write lock before checking/flushing
        drop(state);

        if timing == ReportTiming::InProgress {
            self.check_and_flush_reports();
        }
    }

    fn write_to_csv<T>(&self, element: ElementType, reports: &[T])
    where
        T: Serialize,
    {
        let _guard = self.file_lock.lock().expect("Failed to acquire file lock");

        let csv_file_name = match element {
            ElementType::Source => format!("{}sources.csv", self.log_dir),
            ElementType::Scheduler => format!("{}switches.csv", self.log_dir),
            ElementType::Sink => format!("{}sinks.csv", self.log_dir),
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
    pub fn generate_output_files(&self) {
        let mut state = self.shared_state.write();

        // Write remaining reports
        if !state.source_reports.is_empty() {
            let reports = std::mem::take(&mut state.source_reports);
            self.write_to_csv(ElementType::Source, &reports);
        }

        if !state.scheduler_reports.is_empty() {
            let reports = std::mem::take(&mut state.scheduler_reports);
            self.write_to_csv(ElementType::Scheduler, &reports);
        }

        if !state.sink_reports.is_empty() {
            let reports = std::mem::take(&mut state.sink_reports);
            self.write_to_csv(ElementType::Sink, &reports);

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
    }
}
