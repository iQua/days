//! Implements a Progress struct that generates a progress bar to illustrate the
//! progress of the simulation run, and logs received statistics from all the
//! network elements (switches, packet sources, and packet sinks) into a SQLite
//! database.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;
use rusqlite::{Connection, Result};

use asynchronix::model::{InitializedModel, Model};
use asynchronix::time::{MonotonicTime, Scheduler};

pub struct Progress {
    progress_bar: ProgressBar,
    progress_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
    finished: bool,
}

#[derive(Clone)]
pub struct Report {
    pub name: String,
    pub finished: bool,
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

        Progress {
            progress_bar: pg,
            progress_interval,
            duration,
            num_sources,
            finished_sources: 0,
            finished: false,
        }
    }

    fn create_database(&self) -> Result<()> {
        let conn = Connection::open("statistics.db")?;

        conn.execute(
            "CREATE TABLE sources (
                name TEXT NOT NULL,
                data BLOB
            )",
            (), // empty list of parameters.
        )?;

        conn.execute(
            "CREATE TABLE switches (
                name TEXT NOT NULL,
                data BLOB
            )",
            (), // empty list of parameters.
        )?;

        conn.execute(
            "CREATE TABLE sinks (
                name TEXT NOT NULL,
                data BLOB
            )",
            (), // empty list of parameters.
        )?;

        Ok(())
    }

    fn log_report(&mut self, report: Report) -> Result<()> {
        let conn = Connection::open("statistics.db")?;

        debug!("Progress logged report from {}", report.name);

        let data: Option<f64> = None;
        if report.name.contains("Source") {
            conn.execute(
                "INSERT INTO sources (name, data) VALUES (?1, ?2)",
                (&report.name, data),
            )?;
        } else if report.name.contains("Switch") {
            conn.execute(
                "INSERT INTO switchs (name, data) VALUES (?1, ?2)",
                (&report.name, data),
            )?;
        } else if report.name.contains("Sink") {
            conn.execute(
                "INSERT INTO sinks (name, data) VALUES (?1, ?2)",
                (&report.name, data),
            )?;
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
            let _ = self.create_database();

            self.run((), scheduler);
            self.into()
        })
    }
}
