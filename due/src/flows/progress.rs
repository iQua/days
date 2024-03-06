//! Implements a Progress struct that generates a progress bar to illustrate the
//! progress of the simulation run.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;

use asynchronix::model::{InitializedModel, Model};
use asynchronix::time::{MonotonicTime, Scheduler};

#[derive(Clone, Debug)]
pub struct FinishMsg {}

pub struct Progress {
    progress_bar: ProgressBar,
    progress_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
    finished: bool,
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

    /// Sets up progress interval and duration from a configuration file.
    pub fn setup(progress: Option<f64>, duration: Option<f64>) -> (f64, f64) {
        let duration = duration.unwrap_or(1500.);
        let progress_interval = progress.unwrap_or(duration / 100.);
        (progress_interval, duration)
    }

    pub fn finish_msg_received(&mut self, _finish_msg: FinishMsg, scheduler: &Scheduler<Self>) {
        self.finished_sources += 1;
        debug!(
            "{} / {} sources are finished.",
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
