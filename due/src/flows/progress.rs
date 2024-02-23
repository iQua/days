//! Implements a Progress struct that generates a progress bar to illustrate the
//! progress of the simulation run, and logs received statistics from all the
//! network elements (switches, packet sources, and packet sinks) into a SQLite
//! database.

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;
use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool};

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
    db_pool: SqlitePool,
    db_tables: HashMap<String, (String, usize)>,
}

#[derive(Clone)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

impl Progress {
    pub async fn new(progress_interval: f64, duration: f64, num_sources: usize) -> Progress {
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

        let db_url = String::from("sqlite://output.db");
        let (db_pool, db_tables) = Self::create_database(&db_url).await.unwrap();

        Progress {
            progress_bar: pg,
            progress_interval,
            duration,
            num_sources,
            finished_sources: 0,
            finished: false,
            db_pool,
            db_tables,
        }
    }

    async fn create_database(
        db_url: &str,
    ) -> Result<(SqlitePool, HashMap<String, (String, usize)>), Box<dyn Error>> {
        if !Sqlite::database_exists(&db_url).await.unwrap_or(false) {
            Sqlite::create_database(&db_url).await?;
            debug!("Created database {} to log outputs.", &db_url);
        }
        let db_pool = SqlitePool::connect(&db_url).await?;

        for table in vec!["sources", "switches", "sinks"] {
            sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
                .execute(&db_pool)
                .await?;
        }

        let mut db_tables: HashMap<String, (String, usize)> = Default::default();

        // creates a table for logging reports of PacketSource
        let table_column =
            "id, start_time, end_time, sent_packets, packet_sizes, ack_bytes, finished".to_string();
        let num_column = 7;
        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS sources(
                id              INTEGER NOT NULL,
                start_time      REAL    NOT NULL,
                end_time        REAL    NOT NULL,
                sent_packets    INTEGER NOT NULL,
                packet_sizes    INTEGER NOT NULL,
                ack_bytes       INTEGER NOT NULL,
                finished        BOOLEAN NOT NULL
            )",
        )
        .execute(&db_pool)
        .await?;
        db_tables.insert("sources".to_string(), (table_column, num_column));

        // creates a table for logging reports of PacketSwitch
        let table_column = "id, start_time, end_time, received_packets,
        dropped_packets,forwarded_packets,queue_length,received_sizes,
        forwarded_sizes,throughput_mean,queueing_delay_mean"
            .to_string();
        let num_column = 11;
        sqlx::query!(
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
        )
        .execute(&db_pool)
        .await?;
        db_tables.insert("switches".to_string(), (table_column, num_column));

        // creates a table for logging reports of PacketSink
        let table_column = "id, start_time, end_time, received_packets,
        received_sizes, queueing_delay_mean, one_way_delay_mean"
            .to_string();
        let num_column = 7;
        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS sinks(
                id                     INTEGER NOT NULL,
                start_time             REAL    NOT NULL,
                end_time               REAL    NOT NULL,
                received_packets       INTEGER NOT NULL,
                received_sizes         INTEGER NOT NULL,
                queueing_delay_mean    REAL    NOT NULL,
                one_way_delay_mean     REAL    NOT NULL
            )",
        )
        .execute(&db_pool)
        .await?;
        db_tables.insert("sinks".to_string(), (table_column, num_column));

        Ok((db_pool, db_tables))
    }

    async fn log_report(&mut self, report: Report) -> Result<(), Box<dyn Error>> {
        match report {
            Report::PacketSourceReport(source_report) => {
                debug!(
                    "Progress received report from PacketSource {}",
                    source_report.id
                );
                let mut query_str = "INSERT INTO sources (".to_owned();
                let (column, num_column) = self.db_tables.get("sources").unwrap();
                query_str.push_str(&format!("{column}) VALUES ( $1"));
                for i in 2..=num_column.clone() {
                    query_str.push_str(&format!(", ${i})"));
                }
                sqlx::query(&query_str)
                    .bind(source_report.id)
                    .bind(source_report.start_time)
                    .bind(source_report.end_time)
                    .bind(source_report.sent_packets)
                    .bind(source_report.packet_sizes)
                    .bind(source_report.ack_bytes)
                    .bind(source_report.finished)
                    .execute(&self.db_pool)
                    .await?;
            }
            Report::SchedulerReport(switch_report) => {
                debug!(
                    "Progress received report from Scheduler {}",
                    switch_report.id
                );
                let mut query_str = "INSERT INTO switches (".to_owned();
                let (column, num_column) = self.db_tables.get("switches").unwrap();
                query_str.push_str(&format!("{column}) VALUES ( $1"));
                for i in 2..=num_column.clone() {
                    query_str.push_str(&format!(", ${i})"));
                }
                sqlx::query(&query_str)
                    .bind(switch_report.id)
                    .bind(switch_report.start_time)
                    .bind(switch_report.end_time)
                    .bind(switch_report.received_packets)
                    .bind(switch_report.dropped_packets)
                    .bind(switch_report.forwarded_packets)
                    .bind(switch_report.queue_length)
                    .bind(switch_report.received_sizes)
                    .bind(switch_report.forwarded_sizes)
                    .bind(switch_report.throughput_mean)
                    .bind(switch_report.queueing_delay_mean)
                    .execute(&self.db_pool)
                    .await?;
            }
            Report::PacketSinkReport(sink_report) => {
                debug!(
                    "Progress received report from PacketSink {}",
                    sink_report.id
                );
                let mut query_str = "INSERT INTO sinks (".to_owned();
                let (column, num_column) = self.db_tables.get("sinks").unwrap();
                query_str.push_str(&format!("{column}) VALUES ( $1"));
                for i in 2..=num_column.clone() {
                    query_str.push_str(&format!(", ${i})"));
                }
                sqlx::query(&query_str)
                    .bind(sink_report.id)
                    .bind(sink_report.start_time)
                    .bind(sink_report.end_time)
                    .bind(sink_report.received_packets)
                    .bind(sink_report.received_sizes)
                    .bind(sink_report.queueing_delay_mean)
                    .bind(sink_report.one_way_delay_mean)
                    .execute(&self.db_pool)
                    .await?;
            }
        }

        Ok(())
    }

    pub fn report_received(&mut self, report: Report, scheduler: &Scheduler<Self>) {
        let _ = self.log_report(report.clone());

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
