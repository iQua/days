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

#[derive(Clone, Debug)]
pub struct CsvLogger {
    // The directory to save the log files
    log_dir: String,
    // The maximum number of reports to be saved in the memory before writing to CSV
    max_log_len: usize,
    scheduler_reports: Vec<SchedulerReport>,
    source_reports: Vec<PacketSourceReport>,
    sink_reports: Vec<PacketSinkReport>,
    total_packets: usize,
    total_delay: f64,
}

impl CsvLogger {
    pub fn new(log_path: Option<String>) -> Self {
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

        CsvLogger {
            log_dir,
            max_log_len: 10000,
            scheduler_reports: Vec::new(),
            source_reports: Vec::new(),
            sink_reports: Vec::new(),
            total_packets: 0,
            total_delay: 0.0,
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
                    // Compute packet stats before writing reports
                    self.compute_packet_stats(&self.sink_reports);

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

    fn compute_packet_stats(&mut self, reports: &[PacketSinkReport]) {
        for report in reports {
            self.total_packets += report.received_packets;
            self.total_delay += report.one_way_delay_mean * report.received_packets as f64;
        }
    }

    pub fn generate_output_files(&self) {
        // Update access patterns for thread safety
        self.write_to_csv(ElementType::Source, &self.source_reports);
        self.write_to_csv(ElementType::Scheduler, &self.scheduler_reports);
        self.write_to_csv(ElementType::Sink, &self.sink_reports);

        let avg_delay = if self.total_packets > 0 {
            self.total_delay / self.total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", self.total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);
    }
}
