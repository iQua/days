//! Implements a Progress struct that generates a progress bar to illustrate the
//! progress of the simulation run, and logs received statistics from all the
//! network elements (switches, packet sources, and packet sinks) into a SQLite
//! database.

use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;
use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool};

use asynchronix::model::{InitializedModel, Model};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::sink::PacketSinkStatistics;
use crate::flows::source::PacketSourceStatistics;
use crate::switches::switch::PacketSwitchStatistics;

pub struct Progress {
    progress_bar: ProgressBar,
    progress_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
    finished: bool,
    db_pool: SqlitePool,
}

#[derive(Clone)]
pub struct Report {
    pub name: String,
    pub statistics: PacketStatistics,
    pub finished: bool,
}

#[derive(Clone)]
pub enum PacketStatistics {
    PacketSourceStatistics(PacketSourceStatistics),
    PacketSwitchStatistics(PacketSwitchStatistics),
    PacketSinkStatistics(PacketSinkStatistics),
}

impl Progress {
    pub fn new(progress_interval: f64, duration: f64, num_sources: usize) -> Progress {
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

        let db_url = String::from("sqlite://statistics.db");
        let db_pool = Self::create_database(&db_url).unwrap();

        Progress {
            progress_bar: pg,
            progress_interval,
            duration,
            num_sources,
            finished_sources: 0,
            finished: false,
            db_pool,
        }
    }

    #[tokio::main]
    async fn create_database(db_url: &str) -> Result<SqlitePool, sqlx::Error> {
        if Sqlite::database_exists(&db_url).await.unwrap_or(false) {
            match fs::remove_file(&db_url) {
                Ok(_) => debug!("Existing database is deleted."),
                Err(error) => panic!("Error {} occurred when deleting existing database.", error),
            }
        }
        match Sqlite::create_database(&db_url).await {
            Ok(_) => debug!("Created database {} to log packet statistics.", &db_url),
            Err(error) => panic!("Error {} occurred when creating a database", error),
        }

        let db_pool = SqlitePool::connect("statistics.db").await?;

        let source_qry = "CREATE TABLE sources
        (
            name        TEXT    NOT NULL,
            data        BLOB,
            finished    BOOLEAN NOT NULL DEFAULT 0
        )";
        sqlx::query(source_qry).execute(&db_pool).await?;

        let switch_qry = "CREATE TABLE switches
        (
            name        TEXT    NOT NULL,
            data        BLOB,
            finished    BOOLEAN NOT NULL DEFAULT 0
        )";
        sqlx::query(switch_qry).execute(&db_pool).await?;

        let sink_qry = "CREATE TABLE sinks
        (
            name        TEXT    NOT NULL,
            data        BLOB,
            finished    BOOLEAN NOT NULL DEFAULT 0
        )";
        sqlx::query(sink_qry).execute(&db_pool).await?;

        Ok(db_pool)
    }

    async fn log_report(&mut self, report: Report) -> Result<(), sqlx::Error> {
        debug!("Progress logged report from {}", report.name);

        let data: Option<f64> = None;
        if report.name.contains("Source") {
            sqlx::query("INSERT INTO sources (name, data) VALUES (?1, ?2)")
                .bind(&report.name)
                .bind(data)
                .execute(&self.db_pool)
                .await?;
        } else if report.name.contains("Switch") {
            sqlx::query("INSERT INTO switchs (name, data) VALUES (?1, ?2)")
                .bind(&report.name)
                .bind(data)
                .execute(&self.db_pool)
                .await?;
        } else if report.name.contains("Sink") {
            sqlx::query("INSERT INTO sinks (name, data) VALUES (?1, ?2)")
                .bind(&report.name)
                .bind(data)
                .execute(&self.db_pool)
                .await?;
        }

        Ok(())
    }

    pub fn report_received(&mut self, report: Report, scheduler: &Scheduler<Self>) {
        debug!("Progress received report from {}", report.name);

        let _ = self.log_report(report.clone());

        if report.finished {
            self.finished_sources += 1;
            debug!(
                "{} / {} sources are finished",
                self.finished_sources, self.num_sources
            );
            if self.finished_sources == self.num_sources {
                self.finished = true;

                self.progress_bar.inc(
                    (self.duration / self.progress_interval) as u64 - self.progress_bar.position(),
                );

                let now = scheduler
                    .time()
                    .duration_since(MonotonicTime::EPOCH)
                    .as_secs_f64();

                scheduler
                    .schedule_event(Duration::from_secs_f64(self.duration - now), Self::run, ())
                    .unwrap();
            }
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
