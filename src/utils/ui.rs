//! Implements a UserInterface struct that includes a progress bar to illustrate the
//! progress of the simulation run, as well as a report logger that logs reports
//! to a .csv file.

use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::time::MonotonicTime;

use crate::utils::reporter::Report;
use crate::{get_update_interval, set_update_interval};

pub struct UserInterface {
    progress_bar: ProgressBar,
    update_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
    finished: bool,
}

impl UserInterface {
    pub fn new(update_interval: f64, duration: f64, num_sources: usize) -> UserInterface {
        let multi = MultiProgress::new();
        let logger = env_logger::Builder::from_default_env().build();

        LogWrapper::new(multi.clone(), logger);

        let progress_bar = ProgressBar::new((duration / update_interval) as u64);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:90.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
            )
            .unwrap(),
        );

        let pg = multi.add(progress_bar);

        UserInterface {
            progress_bar: pg,
            update_interval: get_update_interval(),
            duration,
            num_sources,
            finished_sources: 0,
            finished: false,
        }
    }

    /// Sets up progress interval and duration from a configuration file.
    pub fn setup(update_interval: Option<f64>, duration: Option<f64>) -> f64 {
        let duration = duration.unwrap_or(1500.);
        set_update_interval(update_interval.unwrap_or(duration / 100.));

        duration
    }

    pub fn report_arrived(&mut self, _report: Report, cx: &mut Context<Self>) {
        self.finished_sources += 1;
        debug!(
            "{} / {} sources have finished.",
            self.finished_sources, self.num_sources
        );
        if self.finished_sources == self.num_sources {
            self.finished = true;

            self.progress_bar
                .inc((self.duration / self.update_interval) as u64 - self.progress_bar.position());

            let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            cx.schedule_event(Duration::from_secs_f64(self.duration - now), Self::run, ())
                .unwrap();
        }
    }

    fn run(&mut self, _: (), cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        if self.finished {
            if now == self.duration {
                self.progress_bar.finish_and_clear();
            }
        } else {
            self.progress_bar.inc(1);
            if self.progress_bar.position() >= (self.duration / self.update_interval) as u64 {
                self.progress_bar.finish_and_clear();
                self.finished = true;
            } else {
                cx.schedule_event(Duration::from_secs_f64(self.update_interval), Self::run, ())
                    .unwrap();
            }
        }
    }
}

impl Model for UserInterface {
    async fn init(mut self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        self.run((), cx);
        self.into()
    }
}
