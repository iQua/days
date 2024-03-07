//! Implements a report logger to log periodic reports of sources, schedulers,
//! and sinks to a SQLite database or three JSON files.

use log::info;
use rusqlite::Connection;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

#[derive(Deserialize)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum LogType {
    DB,
    JSON,
    NONE,
}

#[derive(Clone, Debug)]
pub enum ReportLogger {
    ReportLoggerDB(ReportLoggerDB),
    ReportLoggerJson(ReportLoggerJson),
    ReportLoggerNone(ReportLoggerNone),
}

impl ReportLogger {
    pub fn new(log_path: Option<String>, log_type: Option<LogType>) -> Self {
        let log_type = log_type.unwrap_or(LogType::NONE);
        let log_path = match log_type {
            LogType::DB => log_path.unwrap_or("./output.db".to_string()),
            LogType::JSON => log_path.unwrap_or("./output/".to_string()),
            LogType::NONE => String::default(),
        };

        match log_type {
            LogType::DB => ReportLogger::ReportLoggerDB(ReportLoggerDB::new(&log_path)),
            LogType::JSON => ReportLogger::ReportLoggerJson(ReportLoggerJson::new(&log_path)),
            LogType::NONE => ReportLogger::ReportLoggerNone(ReportLoggerNone {}),
        }
    }

    /// Returns a void report logger that doesn't log.
    pub fn default() -> Self {
        ReportLogger::ReportLoggerNone(ReportLoggerNone {})
    }

    pub fn log_report(&self, report: Report) {
        match self {
            ReportLogger::ReportLoggerDB(report_logger) => report_logger.log_report(report),
            ReportLogger::ReportLoggerJson(report_logger) => report_logger.log_report(report),
            ReportLogger::ReportLoggerNone(_) => {}
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReportLoggerDB {
    db_path: String,
    db_queries: HashMap<String, String>,
}

impl ReportLoggerDB {
    pub fn new(log_path: &str) -> Self {
        let mut db_path = log_path.to_string();
        if log_path[log_path.len() - 3..].to_string() != ".db" {
            db_path.push_str(".db");
        }

        info!(
            "Outputs of this simulation run will be logged to a SQLite database {}.",
            &db_path
        );

        ReportLoggerDB {
            db_path: db_path.clone(),
            db_queries: Self::create_database(&db_path),
        }
    }

    pub fn create_database(db_path: &str) -> HashMap<String, String> {
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

        if let Err(e) = conn.close() {
            panic!(
                "Error '{:?}' occurred when closing the SQLite connection of {}",
                e, &db_path
            );
        };

        db_queries
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
        let db_conn = match Connection::open(&self.db_path) {
            Ok(conn) => conn,
            Err(e) => panic!(
                "Error '{}' occurred when opening the database {} to log output",
                e, &self.db_path
            ),
        };

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

        if let Err(e) = db_conn.close() {
            panic!(
                "Error '{:?}' occurred when closing the SQLite connection of {}",
                e, &self.db_path
            );
        };
    }
}

#[derive(Clone, Debug)]
pub struct ReportLoggerJson {
    log_file_paths: HashMap<String, String>,
    log_file_locks: HashMap<String, Arc<Mutex<File>>>,
}

impl ReportLoggerJson {
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

        let mut log_file_paths: HashMap<String, String> = Default::default();
        let mut log_file_locks: HashMap<String, Arc<Mutex<File>>> = Default::default();
        for element in vec!["sources", "switches", "sinks"] {
            let file_name = format!("{log_dir}{element}.json");
            log_file_paths.insert(element.to_string(), file_name.clone());

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
            let log_lock = Arc::new(Mutex::new(log_file));
            log_file_locks.insert(element.to_string(), log_lock);
        }

        info!(
            "Outputs of this simulation run will be logged to three JSON files under directory {}.",
            &log_dir
        );

        ReportLoggerJson {
            log_file_paths,
            log_file_locks,
        }
    }

    pub fn log_report(&self, report: Report) {
        let (log_file_path, log_file_lock, report_json) = match report {
            Report::PacketSourceReport(report) => (
                self.log_file_paths.get("sources").unwrap(),
                self.log_file_locks.get("sources").unwrap(),
                serde_json::to_string_pretty(&report).unwrap(),
            ),
            Report::SchedulerReport(report) => (
                self.log_file_paths.get("switches").unwrap(),
                self.log_file_locks.get("switches").unwrap(),
                serde_json::to_string_pretty(&report).unwrap(),
            ),
            Report::PacketSinkReport(report) => (
                self.log_file_paths.get("sinks").unwrap(),
                self.log_file_locks.get("sinks").unwrap(),
                serde_json::to_string_pretty(&report).unwrap(),
            ),
        };

        let log_file_lock = Arc::clone(log_file_lock);
        loop {
            let mut log_lock = log_file_lock.try_lock();
            if let Ok(ref mut log_file) = log_lock {
                if let Err(e) = writeln!(**log_file, "{}", report_json) {
                    panic!(
                        "Error '{}' occurred when writing to log file {}",
                        e, log_file_path
                    );
                };
                break;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReportLoggerNone {}

impl ReportLoggerNone {}
