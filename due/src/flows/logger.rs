//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to a SQLite database or three JSON files.

use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};

use lazy_static::lazy_static;
use log::info;
use rusqlite::Connection;
use serde::Deserialize;

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;

lazy_static! {
    pub static ref LOG_PATH: RwLock<String> = RwLock::new(String::default());
    pub static ref LOG_TYPE: RwLock<LogType> = RwLock::new(LogType::None);
    pub static ref REPORT_INTERVAL: RwLock<f64> = RwLock::new(f64::MAX);
}

pub struct PeriodicLogger {
    report_logger: ReportLogger,
    report_interval: f64,
}

impl PeriodicLogger {
    pub fn new() -> PeriodicLogger {
        PeriodicLogger {
            report_logger: ReportLogger::new(
                LOG_PATH.read().unwrap().clone(),
                *LOG_TYPE.read().unwrap(),
            ),
            report_interval: *REPORT_INTERVAL.read().unwrap(),
        }
    }

    pub fn init(log_path: Option<String>, log_type: Option<LogType>, report_interval: f64) {
        if log_type.is_some() {
            let mut log_type_static = LOG_TYPE.write().unwrap();
            *log_type_static = log_type.unwrap();
        }

        let mut log_path_static = LOG_PATH.write().unwrap();
        if log_path.is_some() {
            *log_path_static = log_path.unwrap();
        } else {
            let default_log_path = match *LOG_TYPE.read().unwrap() {
                LogType::Database => log_path.unwrap_or("./output.db".to_string()),
                LogType::JSON => log_path.unwrap_or("./output/".to_string()),
                LogType::None => String::default(),
            };
            *log_path_static = default_log_path;
        }

        let mut report_interval_static = REPORT_INTERVAL.write().unwrap();
        *report_interval_static = report_interval;
    }

    pub fn get_instance() -> Arc<PeriodicLogger> {
        lazy_static! {
            static ref INSTANCE: Mutex<Option<Arc<PeriodicLogger>>> = Mutex::new(None);
        }

        let mut instance = INSTANCE.lock().unwrap();
        if instance.is_none() {
            *instance = Some(Arc::new(PeriodicLogger::new()));
        }
        Arc::clone(instance.as_ref().unwrap())
    }

    pub fn log_report(report: Report) {
        let report_logger = &PeriodicLogger::get_instance().report_logger;
        report_logger.log_report(report);
    }

    pub fn get_report_interval() -> f64 {
        PeriodicLogger::get_instance().report_interval
    }
}

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum LogType {
    Database,
    JSON,
    None,
}

#[derive(Debug)]
pub enum ReportLogger {
    DatabaseLogger(DatabaseLogger),
    JsonLogger(JsonLogger),
    ReportLoggerNone(ReportLoggerNone),
}

impl ReportLogger {
    pub fn new(log_path: String, log_type: LogType) -> Self {
        match log_type {
            LogType::Database => ReportLogger::DatabaseLogger(DatabaseLogger::new(&log_path)),
            LogType::JSON => ReportLogger::JsonLogger(JsonLogger::new(&log_path)),
            LogType::None => ReportLogger::ReportLoggerNone(ReportLoggerNone {}),
        }
    }

    /// Returns a void report logger that doesn't log.
    pub fn default() -> Self {
        ReportLogger::ReportLoggerNone(ReportLoggerNone {})
    }

    pub fn log_report(&self, report: Report) {
        match self {
            ReportLogger::DatabaseLogger(report_logger) => report_logger.log_report(report),
            ReportLogger::JsonLogger(report_logger) => report_logger.log_report(report),
            ReportLogger::ReportLoggerNone(_) => {}
        }
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseLogger {
    db_lock: Arc<Mutex<Connection>>,
    db_queries: HashMap<String, String>,
}

impl DatabaseLogger {
    pub fn new(log_path: &str) -> Self {
        let mut db_path = log_path.to_string();
        if log_path[log_path.len() - 3..].to_string() != ".db" {
            db_path.push_str(".db");
        }

        info!(
            "Outputs of this simulation run will be logged to a SQLite database {}.",
            &db_path
        );

        let (db_lock, db_queries) = Self::create_database(&db_path);

        DatabaseLogger {
            db_lock,
            db_queries,
        }
    }

    pub fn create_database(db_path: &str) -> (Arc<Mutex<Connection>>, HashMap<String, String>) {
        let conn = match Connection::open(&db_path) {
            Ok(conn) => conn,
            Err(e) => panic!(
                "Error '{}' occurred when creating database {} to log output",
                e, &db_path
            ),
        };

        for table in vec!["sources", "switches", "sinks"] {
            if let Err(e) = conn.execute(&format!("DROP TABLE IF EXISTS {table}"), ()) {
                panic!("Error '{}' occurred when dropping table {}", e, table);
            };
        }

        let mut db_queries: HashMap<String, String> = Default::default();

        // creates a table for logging reports of PacketSource
        if let Err(e) = conn.execute(
            "CREATE TABLE IF NOT EXISTS sources(
                id              INTEGER NOT NULL,
                start_time      REAL    NOT NULL,
                end_time        REAL    NOT NULL,
                sent_packets    INTEGER NOT NULL,
                packet_sizes    INTEGER NOT NULL,
                ack_bytes       INTEGER NOT NULL
            )",
            (),
        ) {
            panic!("Error '{}' occurred when creating table sources", e);
        };
        let table_column =
            "id, start_time, end_time, sent_packets, packet_sizes, ack_bytes".to_string();
        let num_column = 6;
        let query_str = Self::generate_insert_query_str("sources", &table_column, num_column);
        db_queries.insert("sources".to_string(), query_str);

        // creates a table for logging reports of PacketSwitch
        if let Err(e) = conn.execute(
            "CREATE TABLE IF NOT EXISTS switches(
                id                     INTEGER NOT NULL,
                start_time             REAL    NOT NULL,
                end_time               REAL    NOT NULL,
                received_packets       INTEGER NOT NULL,
                dropped_packets        INTEGER NOT NULL,
                forwarded_packets      INTEGER NOT NULL,
                queue_length           INTEGER NOT NULL,
                received_sizes         INTEGER NOT NULL,
                forwarded_sizes        INTEGER NOT NULL,
                throughput_mean        REAL    NOT NULL,
                queueing_delay_mean    REAL    NOT NULL
            )",
            (),
        ) {
            panic!("Error '{}' occurred when creating table switches", e);
        };
        let table_column = "id, start_time, end_time, received_packets,
        dropped_packets,forwarded_packets,queue_length,received_sizes,
        forwarded_sizes,throughput_mean,queueing_delay_mean"
            .to_string();
        let num_column = 11;
        let query_str = Self::generate_insert_query_str("switches", &table_column, num_column);
        db_queries.insert("switches".to_string(), query_str);

        // creates a table for logging reports of PacketSink
        if let Err(e) = conn.execute(
            "CREATE TABLE IF NOT EXISTS sinks(
                id                     INTEGER NOT NULL,
                start_time             REAL    NOT NULL,
                end_time               REAL    NOT NULL,
                received_packets       INTEGER NOT NULL,
                received_sizes         INTEGER NOT NULL,
                queueing_delay_mean    REAL    NOT NULL,
                one_way_delay_mean     REAL    NOT NULL
            )",
            (),
        ) {
            panic!("Error '{}' occurred when creating table sinks", e);
        };
        let table_column = "id, start_time, end_time, received_packets,
        received_sizes, queueing_delay_mean, one_way_delay_mean"
            .to_string();
        let num_column = 7;
        let query_str = Self::generate_insert_query_str("sinks", &table_column, num_column);
        db_queries.insert("sinks".to_string(), query_str);

        (Arc::new(Mutex::new(conn)), db_queries)
    }

    fn generate_insert_query_str(element_type: &str, column: &str, num_column: usize) -> String {
        let mut query_str = format!("INSERT INTO {element_type} (");

        query_str.push_str(&format!("{column}) VALUES ( ?1"));
        for i in 2..=num_column.clone() {
            query_str.push_str(&format!(", ?{i}"));
        }
        query_str.push_str(")");
        query_str
    }

    pub fn log_report(&self, report: Report) {
        let db_lock = Arc::clone(&self.db_lock);
        loop {
            let mut conn_lock = db_lock.try_lock();
            if let Ok(ref mut db_conn) = conn_lock {
                match report {
                    Report::PacketSourceReport(source_report) => {
                        let query_str = self.db_queries.get("sources").unwrap().to_string();
                        if let Err(e) = db_conn.execute(
                            &query_str,
                            (
                                source_report.id,
                                source_report.start_time,
                                source_report.end_time,
                                source_report.sent_packets,
                                source_report.packet_sizes,
                                source_report.ack_bytes,
                            ),
                        ) {
                            panic!("Error '{}' occurred when logging a source report", e);
                        };
                    }
                    Report::SchedulerReport(switch_report) => {
                        let query_str = self.db_queries.get("switches").unwrap().to_string();
                        if let Err(e) = db_conn.execute(
                            &query_str,
                            (
                                switch_report.id,
                                switch_report.start_time,
                                switch_report.end_time,
                                switch_report.received_packets,
                                switch_report.dropped_packets,
                                switch_report.forwarded_packets,
                                switch_report.queue_length,
                                switch_report.received_sizes,
                                switch_report.forwarded_sizes,
                                switch_report.throughput_mean,
                                switch_report.queueing_delay_mean,
                            ),
                        ) {
                            panic!("Error '{}' occurred when logging a switch report", e);
                        };
                    }
                    Report::PacketSinkReport(sink_report) => {
                        let query_str = self.db_queries.get("sinks").unwrap().to_string();
                        if let Err(e) = db_conn.execute(
                            &query_str,
                            (
                                sink_report.id,
                                sink_report.start_time,
                                sink_report.end_time,
                                sink_report.received_packets,
                                sink_report.received_sizes,
                                sink_report.queueing_delay_mean,
                                sink_report.one_way_delay_mean,
                            ),
                        ) {
                            panic!("Error '{}' occurred when logging a sink report", e);
                        };
                    }
                }

                break;
            }
        }
    }
}

#[derive(Debug)]
pub struct JsonLogger {
    log_files: HashMap<String, File>,
}

impl JsonLogger {
    pub fn new(log_path: &str) -> Self {
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

        let mut log_files: HashMap<String, File> = Default::default();
        for element in vec!["sources", "switches", "sinks"] {
            let file_name = format!("{log_dir}{element}.json");
            if let Err(e) = File::create(&file_name) {
                panic!(
                    "Error '{}' occurred when creating a log file {}",
                    e, &file_name
                );
            };
            let log_file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(file_name)
                .unwrap();
            log_files.insert(element.to_string(), log_file);
        }

        info!(
            "Outputs of this simulation run will be logged to three JSON files under directory {}.",
            &log_dir
        );

        JsonLogger { log_files }
    }

    pub fn log_report(&self, report: Report) {
        let (mut log_file, report_json) = match report {
            Report::PacketSourceReport(report) => (
                self.log_files.get("sources").unwrap(),
                serde_json::to_string_pretty(&report).unwrap(),
            ),
            Report::SchedulerReport(report) => (
                self.log_files.get("switches").unwrap(),
                serde_json::to_string_pretty(&report).unwrap(),
            ),
            Report::PacketSinkReport(report) => (
                self.log_files.get("sinks").unwrap(),
                serde_json::to_string_pretty(&report).unwrap(),
            ),
        };

        if let Err(e) = writeln!(log_file, "{}", report_json) {
            panic!("Error '{}' occurred when writing to a log file", e);
        };
    }
}

#[derive(Debug)]
pub struct ReportLoggerNone {}

impl ReportLoggerNone {}
