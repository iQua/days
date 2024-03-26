//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to three CSV files.

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};

use csv::Writer;
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
    pub static ref LOG_PATH: RwLock<String> = RwLock::new(String::default());
    pub static ref REPORT_INTERVAL: RwLock<f64> = RwLock::new(f64::MAX);
    pub static ref SOURCE_REPORTS: RwLock<Vec<PacketSourceReport>> = RwLock::new(Vec::new());
    pub static ref SCHEDULER_REPORTS: RwLock<Vec<SchedulerReport>> = RwLock::new(Vec::new());
    pub static ref SINK_REPORTS: RwLock<Vec<PacketSinkReport>> = RwLock::new(Vec::new());
}

pub struct ReportLogger {
    report_logger: CsvLogger,
    report_interval: f64,
}

impl ReportLogger {
    pub fn new() -> ReportLogger {
        ReportLogger {
            report_logger: CsvLogger::new(LOG_PATH.read().unwrap().clone()),
            report_interval: *REPORT_INTERVAL.read().unwrap(),
        }
    }

    pub fn init(log_path: Option<String>, report_interval: f64) {
        let mut log_path_static = LOG_PATH.write().unwrap();
        *log_path_static = log_path.unwrap_or("./output/".to_string());

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
pub struct CsvLogger {
    log_files: HashMap<String, String>,
}

impl CsvLogger {
    pub fn new(log_path: String) -> Self {
        let mut log_dir = log_path.to_string();
        if log_dir.chars().last().unwrap() != '/' {
            log_dir.push('/');
        }
        if let Err(e) = create_dir_all(&log_dir) {
            panic!(
                "Error '{}' occurred when creating a directory {} for log files",
                e, &log_dir
            );
        };

        let mut log_files: HashMap<String, String> = Default::default();
        for element in vec!["sources", "switches", "sinks"] {
            let file_name = format!("{log_dir}{element}.csv");
            log_files.insert(element.to_string(), file_name.clone());
        }

        info!(
            "Outputs of this simulation run will be logged to three csv files under directory {}.",
            &log_dir
        );

        CsvLogger { log_files }
    }

    pub fn log_report(&self, report: Report) {
        match report {
            Report::PacketSourceReport(report) => {
                let mut reports = SOURCE_REPORTS.write().unwrap();
                reports.push(report);
            }
            Report::SchedulerReport(report) => {
                let mut reports = SCHEDULER_REPORTS.write().unwrap();
                reports.push(report);
            }
            Report::PacketSinkReport(report) => {
                let mut reports = SINK_REPORTS.write().unwrap();
                reports.push(report);
            }
        };
    }

    fn write_to_csv<T>(&self, element_type: &str, reports: &Vec<T>)
    where
        T: serde::Serialize,
    {
        let csv_file = self.log_files.get(element_type).unwrap();
        let mut csv_writer = match Writer::from_path(csv_file) {
            Ok(wtr) => wtr,
            Err(e) => {
                panic!(
                    "Error '{}' occurred when writing to csv file {}",
                    e, &csv_file
                );
            }
        };

        for report in reports {
            if let Err(e) = csv_writer.serialize(report) {
                panic!(
                    "Error '{}' occurred when writing a report to csv file {}",
                    e, &csv_file
                );
            }
        }
    }

    pub fn generate_output_files(&mut self) {
        let reports = SOURCE_REPORTS.read().unwrap();
        self.write_to_csv("sources", &reports);

        let reports = SCHEDULER_REPORTS.read().unwrap();
        self.write_to_csv("switches", &reports);

        let reports = SINK_REPORTS.read().unwrap();
        self.write_to_csv("sinks", &reports);
    }
}
