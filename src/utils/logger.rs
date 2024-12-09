//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use std::fs::{create_dir_all, File, OpenOptions};
use std::sync::{Arc, LazyLock, Mutex, RwLock};

use csv::WriterBuilder;
use log::info;

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;
use crate::utils::ui::{Report, ReportTiming};

enum ElementType {
    Source,
    Scheduler,
    Sink,
}

pub static LOG_FILES_DIR: LazyLock<RwLock<String>> =
    LazyLock::new(|| RwLock::new("./output/".to_string()));
pub static REPORT_INTERVAL: LazyLock<RwLock<f64>> = LazyLock::new(|| RwLock::new(f64::MAX));
pub static SOURCE_REPORTS: LazyLock<RwLock<Vec<PacketSourceReport>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));
pub static SCHEDULER_REPORTS: LazyLock<RwLock<Vec<SchedulerReport>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));
pub static SINK_REPORTS: LazyLock<RwLock<Vec<PacketSinkReport>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));
pub static TOTAL_PACKETS: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(0));
pub static TOTAL_DELAY: LazyLock<RwLock<f64>> = LazyLock::new(|| RwLock::new(0.0));

pub struct ReportLogger {
    report_logger: CsvLogger,
}

impl ReportLogger {
    pub fn new() -> ReportLogger {
        ReportLogger {
            report_logger: CsvLogger {},
        }
    }

    pub fn init(log_path: Option<String>, report_interval: f64) {
        let mut log_dir = log_path.unwrap_or("./output/".to_string());
        if log_dir.chars().last().unwrap() != '/' {
            log_dir.push('/');
        }

        if let Err(e) = create_dir_all(&log_dir) {
            panic!(
                "Error '{}' occurred when creating directory {} for log files",
                e, &log_dir
            );
        };

        // Create output files
        for element in ["sources", "switches", "sinks"] {
            let file_name = format!("{log_dir}{element}.csv");
            if let Err(e) = File::create(&file_name) {
                panic!(
                    "Error '{}' occurred when creating log file {}",
                    e, &file_name
                );
            }
        }

        info!(
            "Outputs of this simulation run will be logged to three CSV files under directory {}.",
            &log_dir
        );

        // Store configs using RwLock only
        *LOG_FILES_DIR.write().unwrap() = log_dir;
        *REPORT_INTERVAL.write().unwrap() = report_interval;
    }

    pub fn get_instance() -> Arc<ReportLogger> {
        static INSTANCE: LazyLock<Mutex<Option<Arc<ReportLogger>>>> =
            LazyLock::new(|| Mutex::new(None));

        let mut instance = INSTANCE.lock().unwrap();
        if instance.is_none() {
            *instance = Some(Arc::new(ReportLogger::new()));
        }
        Arc::clone(instance.as_ref().unwrap())
    }

    pub fn generate_output_files() {
        let report_logger = &ReportLogger::get_instance().report_logger;
        report_logger.generate_output_files();
    }
}

#[derive(Clone, Debug)]
pub struct CsvLogger {}

impl CsvLogger {
    fn logging_due(&self, log_len: usize, timing: ReportTiming) -> bool {
        let max_log_len = 10000;

        match timing {
            ReportTiming::InProgress => log_len >= max_log_len,
            ReportTiming::Final => true,
        }
    }

    pub fn update_ui(&self, report: Report, timing: ReportTiming) {
        match report {
            Report::PacketSourceReport(report) => {
                let mut reports = SOURCE_REPORTS.write().unwrap();
                reports.push(report);
                if self.logging_due(reports.len(), timing) {
                    self.write_to_csv(ElementType::Source, &reports);
                    reports.clear();
                }
            }
            Report::SchedulerReport(report) => {
                let mut reports = SCHEDULER_REPORTS.write().unwrap();
                reports.push(report);
                if self.logging_due(reports.len(), timing) {
                    self.write_to_csv(ElementType::Scheduler, &reports);
                    reports.clear();
                }
            }
            Report::PacketSinkReport(report) => {
                let mut reports = SINK_REPORTS.write().unwrap();
                reports.push(report);
                if self.logging_due(reports.len(), timing) {
                    // Compute packet stats before writing reports
                    self.compute_packet_stats(&reports);

                    self.write_to_csv(ElementType::Sink, &reports);
                    reports.clear();
                }
            }
        };
    }

    fn write_to_csv<T>(&self, element: ElementType, reports: &Vec<T>)
    where
        T: serde::Serialize,
    {
        let log_dir = LOG_FILES_DIR.read().unwrap();

        let csv_file_name = match element {
            ElementType::Source => {
                format!("{}sources.csv", log_dir)
            }
            ElementType::Scheduler => {
                format!("{}switches.csv", log_dir)
            }
            ElementType::Sink => {
                format!("{}sinks.csv", log_dir)
            }
        };

        let csv_file = OpenOptions::new()
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

    fn compute_packet_stats(&self, reports: &[PacketSinkReport]) {
        let mut total_packets = TOTAL_PACKETS.write().unwrap();
        let mut total_delay = TOTAL_DELAY.write().unwrap();

        for report in reports {
            *total_packets += report.received_packets;
            *total_delay += report.one_way_delay_mean * report.received_packets as f64;
        }
    }

    pub fn generate_output_files(&self) {
        // Update access patterns for thread safety
        let reports = SOURCE_REPORTS.read().unwrap();
        self.write_to_csv(ElementType::Source, &reports);

        let reports = SCHEDULER_REPORTS.read().unwrap();
        self.write_to_csv(ElementType::Scheduler, &reports);

        let reports = SINK_REPORTS.read().unwrap();
        self.write_to_csv(ElementType::Sink, &reports);

        let total_packets = *TOTAL_PACKETS.read().unwrap();
        let total_delay = *TOTAL_DELAY.read().unwrap();
        let avg_delay = if total_packets > 0 {
            total_delay / total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);
    }
}
