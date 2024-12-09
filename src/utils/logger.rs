//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use std::fs;

use csv::WriterBuilder;
use log::info;
use serde::Deserialize;

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;
use crate::utils::ui::{Report, ReportTiming};

#[derive(Deserialize)]
struct LogConfig {
    log_path: Option<String>,
}

enum ElementType {
    Source,
    Scheduler,
    Sink,
}

#[derive(Clone, Debug)]
pub struct CsvLogger {
    log_dir: String,
    max_log_len: usize,
    scheduler_reports: Vec<SchedulerReport>,
    source_reports: Vec<PacketSourceReport>,
    sink_reports: Vec<PacketSinkReport>,
}

impl CsvLogger {
    pub fn new(file_path: String) -> Self {
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

        info!(
            "Outputs of this simulation run will be logged to three CSV files under directory {}.",
            &log_dir
        );

        CsvLogger {
            log_dir: "./output/".to_string(),
            max_log_len: 10000,
            scheduler_reports: Vec::new(),
            source_reports: Vec::new(),
            sink_reports: Vec::new(),
        }
    }

    fn logging_due(&self, log_len: usize, timing: ReportTiming) -> bool {
        match timing {
            ReportTiming::InProgress => log_len >= self.max_log_len,
            ReportTiming::Final => true,
        }
    }

    pub fn log_report(&mut self, report: Report, timing: ReportTiming) {
        match report {
            Report::PacketSourceReport(report) => {
                self.source_reports.push(report);
                if self.logging_due(self.source_reports.len(), timing) {
                    self.write_to_csv(ElementType::Source, &self.source_reports);
                    self.source_reports.clear();
                }
            }
            Report::SchedulerReport(report) => {
                self.scheduler_reports.push(report);
                if self.logging_due(self.scheduler_reports.len(), timing) {
                    self.write_to_csv(ElementType::Scheduler, &self.scheduler_reports);
                    self.scheduler_reports.clear();
                }
            }
            Report::PacketSinkReport(report) => {
                self.sink_reports.push(report);
                if self.logging_due(self.sink_reports.len(), timing) {
                    self.write_to_csv(ElementType::Sink, &self.sink_reports);
                    self.sink_reports.clear();
                }
            }
        };
    }

    fn write_to_csv<T>(&self, element: ElementType, reports: &Vec<T>)
    where
        T: serde::Serialize,
    {
        let csv_file_name = match element {
            ElementType::Source => {
                format!("{}sources.csv", self.log_dir)
            }
            ElementType::Scheduler => {
                format!("{}switches.csv", self.log_dir)
            }
            ElementType::Sink => {
                format!("{}sinks.csv", self.log_dir)
            }
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

    pub fn generate_output_files(&self) {
        // Write remaining reports to files
        self.write_to_csv(ElementType::Source, &self.source_reports);
        self.write_to_csv(ElementType::Scheduler, &self.scheduler_reports);
        self.write_to_csv(ElementType::Sink, &self.sink_reports);

        // Compute statistics using functional operations
        let total_packets = self
            .sink_reports
            .iter()
            .map(|report| report.received_packets)
            .sum::<usize>();

        let total_delay = self
            .sink_reports
            .iter()
            .map(|report| report.one_way_delay_mean * report.received_packets as f64)
            .sum::<f64>();

        let avg_delay = if total_packets > 0 {
            total_delay / total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);
    }
}
