//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use std::fs::{create_dir_all, File, OpenOptions};
use std::sync::RwLock;
use std::sync::{Arc, Mutex};

use csv::WriterBuilder;
use lazy_static::lazy_static;
use log::info;

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

lazy_static! {
    pub static ref LOG_FILES_DIR: RwLock<String> = RwLock::new(String::default());
    pub static ref REPORT_INTERVAL: RwLock<f64> = RwLock::new(f64::MAX);
    pub static ref SOURCE_REPORTS: RwLock<Vec<PacketSourceReport>> = RwLock::new(Vec::new());
    pub static ref SCHEDULER_REPORTS: RwLock<Vec<SchedulerReport>> = RwLock::new(Vec::new());
    pub static ref SINK_REPORTS: RwLock<Vec<PacketSinkReport>> = RwLock::new(Vec::new());
    pub static ref SET_SOURCE_FILE_HEADER: RwLock<bool> = RwLock::new(true);
    pub static ref SET_SCHEDULER_FILE_HEADER: RwLock<bool> = RwLock::new(true);
    pub static ref SET_SINK_FILE_HEADER: RwLock<bool> = RwLock::new(true);
}

pub struct ReportLogger {
    report_logger: CsvLogger,
    report_interval: f64,
}

impl ReportLogger {
    pub fn new() -> ReportLogger {
        ReportLogger {
            report_logger: CsvLogger {},
            report_interval: *REPORT_INTERVAL.read().unwrap(),
        }
    }

    pub fn init(log_path: Option<String>, report_interval: f64) {
        let mut log_dir = log_path.unwrap_or("./output/".to_string());
        if log_dir.chars().last().unwrap() != '/' {
            log_dir.push('/');
        }

        if let Err(e) = create_dir_all(&log_dir) {
            panic!(
                "Error '{}' occurred when creating a directory {} for log files",
                e, &log_dir
            );
        };
        for element in vec!["sources", "switches", "sinks"] {
            let file_name = format!("{log_dir}{element}.csv");
            if let Err(e) = File::create(&file_name) {
                panic!(
                    "Error '{}' occurred when creating a log file {}",
                    e, &file_name
                );
            };
        }
        info!(
            "Outputs of this simulation run will be logged to three CSV files under directory {}.",
            &log_dir
        );

        let mut log_path_static = LOG_FILES_DIR.write().unwrap();
        *log_path_static = log_dir;

        let mut report_interval_static = REPORT_INTERVAL.write().unwrap();
        *report_interval_static = report_interval;
    }

    pub fn get_instance() -> Arc<ReportLogger> {
        lazy_static! {
            static ref INSTANCE: Mutex<Option<Arc<ReportLogger>>> = Mutex::new(None);
        }

        let mut instance = INSTANCE.lock().unwrap();
        if instance.is_none() {
            *instance = Some(Arc::new(ReportLogger::new()));
        }
        Arc::clone(instance.as_ref().unwrap())
    }

    pub fn log_report(report: Report) {
        let report_logger = &ReportLogger::get_instance().report_logger;
        report_logger.log_report(report);
    }

    pub fn get_report_interval() -> f64 {
        ReportLogger::get_instance().report_interval
    }
}

#[derive(Clone, Debug)]
pub struct CsvLogger {}

impl CsvLogger {
    pub fn log_report(&self, report: Report) {
        let max_log_num = 10000;

        match report {
            Report::PacketSourceReport(report) => {
                let mut reports = SOURCE_REPORTS.write().unwrap();
                reports.push(report);
                if reports.len() >= max_log_num {
                    self.write_to_csv("source", &reports);
                    reports.clear();
                }
            }
            Report::SchedulerReport(report) => {
                let mut reports = SCHEDULER_REPORTS.write().unwrap();
                reports.push(report);
                if reports.len() >= max_log_num {
                    self.write_to_csv("scheduler", &reports);
                    reports.clear();
                }
            }
            Report::PacketSinkReport(report) => {
                let mut reports = SINK_REPORTS.write().unwrap();
                reports.push(report);
                if reports.len() >= max_log_num {
                    self.write_to_csv("sink", &reports);
                    reports.clear();
                }
            }
        };
    }

    fn write_to_csv<T>(&self, element: &str, reports: &Vec<T>)
    where
        T: serde::Serialize,
    {
        let log_dir = LOG_FILES_DIR.read().unwrap();

        let (set_header, csv_file_name) = if element == "source" {
            let mut set_header_bool = SET_SOURCE_FILE_HEADER.write().unwrap();
            let set_header = set_header_bool.clone();
            *set_header_bool = false;
            (set_header, format!("{log_dir}sources.csv"))
        } else if element == "scheduler" {
            let mut set_header_bool = SET_SCHEDULER_FILE_HEADER.write().unwrap();
            let set_header = set_header_bool.clone();
            *set_header_bool = false;
            (set_header, format!("{log_dir}switches.csv"))
        } else {
            let mut set_header_bool = SET_SINK_FILE_HEADER.write().unwrap();
            let set_header = set_header_bool.clone();
            *set_header_bool = false;
            (set_header, format!("{log_dir}sinks.csv"))
        };

        let csv_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&csv_file_name)
            .unwrap();
        let mut csv_writer = WriterBuilder::new()
            .has_headers(set_header)
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

    pub fn generate_output_files(&mut self) {
        let reports = SOURCE_REPORTS.read().unwrap();
        self.write_to_csv("source", &reports);

        let reports = SCHEDULER_REPORTS.read().unwrap();
        self.write_to_csv("scheduler", &reports);

        let reports = SINK_REPORTS.read().unwrap();
        self.write_to_csv("sink", &reports);

        let log_dir = LOG_FILES_DIR.read().unwrap();
        info!(
            "Wrote outputs of this simulation run to three csv files under directory {}.",
            &log_dir
        );
    }
}
