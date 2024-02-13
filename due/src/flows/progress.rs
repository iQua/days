//! Implements a Progress struct that generates a progress bar to illustrate the
//! progress of the simulation run, and logs received statistics from all the
//! network elements (switches, packet sources, and packet sinks) into a SQLite
//! database.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;

use asynchronix::model::{InitializedModel, Model};
use asynchronix::time::Scheduler;

pub struct Progress {
    progress_bar: ProgressBar,
    progress_interval: u64,
    duration: u64,
}

impl Progress {
    pub fn new(progress_interval: u64, duration: u64) -> Progress {
        let multi = MultiProgress::new();
        let logger = env_logger::Builder::from_default_env().build();

        LogWrapper::new(multi.clone(), logger);

        let progress_bar = ProgressBar::new(duration);
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
        }
    }

    fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        self.progress_bar.inc(self.progress_interval);
        if self.progress_bar.position() >= self.duration {
            self.progress_bar.finish_and_clear();
        } else {
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.progress_interval as f64),
                    Self::run,
                    (),
                )
                .unwrap();
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
