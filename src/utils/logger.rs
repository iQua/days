//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use std::fs;

use csv::WriterBuilder;
use log::info;
use serde::{Deserialize, Serialize};

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

#[derive(Serialize)]
struct SourceReportForCsv {
    pub id: usize,
    pub flow_id: usize,
    pub start_time: f64,
    pub end_time: f64,
    pub sent_packets: usize,
    pub packet_sizes: usize,
    pub ack_bytes: usize,
}

#[derive(Serialize)]
struct SchedulerReportForCsv {
    pub id: usize,
    pub start_time: f64,
    pub end_time: f64,
    pub received_packets: usize,
    pub dropped_packets: usize,
    pub forwarded_packets: usize,
    pub queue_length: usize,
    pub received_sizes: usize,
    pub forwarded_sizes: usize,
    pub throughput_mean: f64,
    pub queueing_delay_mean: f64,
}

#[derive(Serialize)]
struct SinkReportForCsv {
    pub id: usize,
    pub flow_id: usize,
    pub start_time: f64,
    pub end_time: f64,
    pub received_packets: usize,
    pub received_sizes: usize,
    pub queueing_delay_mean: f64,
    pub one_way_delay_mean: f64,
}

impl From<PacketSourceReport> for SourceReportForCsv {
    fn from(report: PacketSourceReport) -> Self {
        Self {
            id: report.id,
            flow_id: report.flow_id,
            start_time: report.start_time,
            end_time: report.end_time,
            sent_packets: report.sent_packets,
            packet_sizes: report.packet_sizes,
            ack_bytes: report.ack_bytes,
        }
    }
}

impl From<PacketSinkReport> for SinkReportForCsv {
    fn from(report: PacketSinkReport) -> Self {
        Self {
            id: report.id,
            flow_id: report.flow_id,
            start_time: report.start_time,
            end_time: report.end_time,
            received_packets: report.received_packets,
            received_sizes: report.received_sizes,
            queueing_delay_mean: report.queueing_delay_mean,
            one_way_delay_mean: report.one_way_delay_mean,
        }
    }
}

impl From<SchedulerReport> for SchedulerReportForCsv {
    fn from(report: SchedulerReport) -> Self {
        Self {
            id: report.id,
            start_time: report.start_time,
            end_time: report.end_time,
            received_packets: report.received_packets,
            dropped_packets: report.dropped_packets,
            forwarded_packets: report.forwarded_packets,
            queue_length: report.queue_length,
            received_sizes: report.received_sizes,
            forwarded_sizes: report.forwarded_sizes,
            throughput_mean: report.throughput_mean,
            queueing_delay_mean: report.queueing_delay_mean,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CsvLogger {
    log_dir: String,
    max_log_len: usize,
    scheduler_reports: Vec<SchedulerReport>,
    source_reports: Vec<PacketSourceReport>,
    sink_reports: Vec<PacketSinkReport>,
    total_packets: usize,
    total_delay: f64,
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
                    self.write_to_csv::<PacketSourceReport, SourceReportForCsv>(
                        ElementType::Source,
                        &self.source_reports,
                    );
                    self.source_reports.clear();
                }
            }
            Report::SchedulerReport(report) => {
                self.scheduler_reports.push(report);
                if self.logging_due(self.scheduler_reports.len(), timing) {
                    self.write_to_csv::<SchedulerReport, SchedulerReportForCsv>(
                        ElementType::Scheduler,
                        &self.scheduler_reports,
                    );
                    self.scheduler_reports.clear();
                }
            }
            Report::PacketSinkReport(report) => {
                self.sink_reports.push(report);
                if self.logging_due(self.sink_reports.len(), timing) {
                    self.write_to_csv::<PacketSinkReport, SinkReportForCsv>(
                        ElementType::Sink,
                        &self.sink_reports,
                    );
                    let (new_packets, new_delay) = self.compute_sink_statistics();
                    self.total_packets += new_packets;
                    self.total_delay += new_delay;
                    self.sink_reports.clear();
                }
            }
        };
    }

    fn write_to_csv<T, U>(&self, element: ElementType, reports: &Vec<T>)
    where
        T: Clone,
        U: Serialize + From<T>,
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
            let csv_report = U::from(report.clone());
            if let Err(e) = csv_writer.serialize(csv_report) {
                panic!(
                    "Error '{}' occurred when writing a report to csv file {}",
                    e, &csv_file_name
                );
            }
        }
    }

    fn compute_sink_statistics(&self) -> (usize, f64) {
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

        (total_packets, total_delay)
    }

    pub fn generate_output_files(&mut self) {
        // Write remaining reports to files
        self.write_to_csv::<PacketSourceReport, SourceReportForCsv>(
            ElementType::Source,
            &self.source_reports,
        );
        self.write_to_csv::<SchedulerReport, SchedulerReportForCsv>(
            ElementType::Scheduler,
            &self.scheduler_reports,
        );
        self.write_to_csv::<PacketSinkReport, SinkReportForCsv>(
            ElementType::Sink,
            &self.sink_reports,
        );

        let (final_packets, final_delay) = self.compute_sink_statistics();
        self.total_packets += final_packets;
        self.total_delay += final_delay;

        let avg_delay = if self.total_packets > 0 {
            self.total_delay / self.total_packets as f64
        } else {
            0.0
        };

        info!("Total packets processed: {}", self.total_packets);
        info!("Average one-way delay: {:.6} seconds", avg_delay);
    }
}
