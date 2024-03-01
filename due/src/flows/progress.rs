//! Implements a Progress struct that generates a progress bar to illustrate the
//! progress of the simulation run, and logs received statistics from all the
//! network elements (switches, packet sources, and packet sinks) into a SQLite
//! database.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;
use rusqlite::Connection;

use asynchronix::model::{InitializedModel, Model};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;

pub struct Progress {
    progress_bar: ProgressBar,
    progress_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
    finished: bool,
    db_conn: Connection,
    db_tables: HashMap<String, (String, usize)>,
}

#[derive(Clone)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

impl Progress {
    pub fn new(
        progress_interval: f64,
        duration: f64,
        num_sources: usize,
        db_path: &str,
    ) -> Progress {
        let multi = MultiProgress::new();
        let logger = env_logger::Builder::from_default_env().build();

        LogWrapper::new(multi.clone(), logger);

        let progress_bar = ProgressBar::new((duration / progress_interval) as u64);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:90.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
            )
            .unwrap(),
        );

        let pg = multi.add(progress_bar);

        let (db_conn, db_tables) = Self::create_database(&db_path);

        Progress {
            progress_bar: pg,
            progress_interval,
            duration,
            num_sources,
            finished_sources: 0,
            finished: false,
            db_conn,
            db_tables,
        }
    }

    fn create_database(db_path: &str) -> (Connection, HashMap<String, (String, usize)>) {
        let conn = match Connection::open(&db_path) {
            Ok(conn) => conn,
            Err(e) => panic!(
                "Error {} occurred when creating database {} to log output",
                e, &db_path
            ),
        };

        for table in vec!["sources", "switches", "sinks"] {
            match conn.execute(&format!("DROP TABLE IF EXISTS {table}"), ()) {
                Ok(_) => {}
                Err(e) => panic!("Error {} occurred when dropping table {}", e, table),
            };
        }

        let mut db_tables: HashMap<String, (String, usize)> = Default::default();

        // creates a table for logging reports of PacketSource
        let table_column =
            "id, start_time, end_time, sent_packets, packet_sizes, ack_bytes, finished".to_string();
        let num_column = 7;
        match conn.execute(
            "CREATE TABLE IF NOT EXISTS sources(
                id              INTEGER NOT NULL,
                start_time      REAL    NOT NULL,
                end_time        REAL    NOT NULL,
                sent_packets    INTEGER NOT NULL,
                packet_sizes    INTEGER NOT NULL,
                ack_bytes       INTEGER NOT NULL,
                finished        BOOLEAN NOT NULL
            )",
            (),
        ) {
            Ok(_) => {}
            Err(e) => panic!("Error {} occurred when creating table sources", e),
        };
        db_tables.insert("sources".to_string(), (table_column, num_column));

        // creates a table for logging reports of PacketSwitch
        let table_column = "id, start_time, end_time, received_packets,
        dropped_packets,forwarded_packets,queue_length,received_sizes,
        forwarded_sizes,throughput_mean,queueing_delay_mean"
            .to_string();
        let num_column = 11;
        match conn.execute(
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
            Ok(_) => {}
            Err(e) => panic!("Error {} occurred when creating table switches", e),
        };
        db_tables.insert("switches".to_string(), (table_column, num_column));

        // creates a table for logging reports of PacketSink
        let table_column = "id, start_time, end_time, received_packets,
        received_sizes, queueing_delay_mean, one_way_delay_mean"
            .to_string();
        let num_column = 7;
        match conn.execute(
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
            Ok(_) => {}
            Err(e) => panic!("Error {} occurred when creating table sinks", e),
        };
        db_tables.insert("sinks".to_string(), (table_column, num_column));

        (conn, db_tables)
    }

    fn generate_insert_query_string(&self, element_type: &str) -> String {
        let mut query_str = format!("INSERT INTO {element_type} (");

        let (column, num_column) = self.db_tables.get(element_type).unwrap();
        query_str.push_str(&format!("{column}) VALUES ( ?1"));
        for i in 2..=num_column.clone() {
            query_str.push_str(&format!(", ?{i}"));
        }
        query_str.push_str(")");
        query_str
    }

    fn log_report(&mut self, report: Report) {
        match report {
            Report::PacketSourceReport(source_report) => {
                debug!(
                    "Progress received report from PacketSource {}",
                    source_report.id
                );
                let query_str = self.generate_insert_query_string("sources");
                match self.db_conn.execute(
                    &query_str,
                    (
                        source_report.id,
                        source_report.start_time,
                        source_report.end_time,
                        source_report.sent_packets,
                        source_report.packet_sizes,
                        source_report.ack_bytes,
                        source_report.finished,
                    ),
                ) {
                    Ok(_) => {}
                    Err(e) => panic!("Error {} occurred when inserting data into sources", e),
                };
            }
            Report::SchedulerReport(switch_report) => {
                debug!(
                    "Progress received report from Scheduler {}",
                    switch_report.id
                );
                let query_str = self.generate_insert_query_string("switches");
                match self.db_conn.execute(
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
                    Ok(_) => {}
                    Err(e) => panic!("Error {} occurred when inserting data into switches", e),
                };
            }
            Report::PacketSinkReport(sink_report) => {
                debug!(
                    "Progress received report from PacketSink {}",
                    sink_report.id
                );
                let query_str = self.generate_insert_query_string("sinks");
                match self.db_conn.execute(
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
                    Ok(_) => {}
                    Err(e) => panic!("Error {} occurred when inserting data into sinks", e),
                };
            }
        }
    }

    pub fn report_received(&mut self, report: Report, scheduler: &Scheduler<Self>) {
        self.log_report(report.clone());

        match report {
            Report::PacketSourceReport(source_report) => {
                if source_report.finished {
                    self.finished_sources += 1;
                    debug!(
                        "{} / {} sources are finished.",
                        self.finished_sources, self.num_sources
                    );
                    if self.finished_sources == self.num_sources {
                        self.finished = true;

                        self.progress_bar.inc(
                            (self.duration / self.progress_interval) as u64
                                - self.progress_bar.position(),
                        );

                        let now = scheduler
                            .time()
                            .duration_since(MonotonicTime::EPOCH)
                            .as_secs_f64();

                        scheduler
                            .schedule_event(
                                Duration::from_secs_f64(self.duration - now),
                                Self::run,
                                (),
                            )
                            .unwrap();
                    }
                }
            }
            Report::SchedulerReport(_) => {}
            Report::PacketSinkReport(_) => {}
        }
    }

    fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        if self.finished {
            if now == self.duration {
                self.progress_bar.finish_and_clear();
            }
        } else {
            self.progress_bar.inc(1);
            if self.progress_bar.position() >= (self.duration / self.progress_interval) as u64 {
                self.progress_bar.finish_and_clear();
                self.finished = true;
            } else {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(self.progress_interval),
                        Self::run,
                        (),
                    )
                    .unwrap();
            }
        }
    }
}

impl Model for Progress {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            self.run((), scheduler);
            self.into()
        })
    }
}
