//! Implements a UserInterface struct that includes a progress bar to illustrate the
//! progress of the simulation run, as well as a report logger that logs reports
//! to a .csv file.

use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;
use serde::Serialize;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::time::MonotonicTime;

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::get_update_interval;
use crate::schedulers::SchedulerReport;
use crate::utils::logger::CsvLogger;

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum ReportTiming {
    InProgress,
    Final,
}

pub struct UserInterface {
    logger: CsvLogger,
    progress_bar: ProgressBar,
    update_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
}

impl UserInterface {
    pub fn new(duration: f64, num_sources: usize, file_path: String) -> UserInterface {
        let multi = MultiProgress::new();
        let env_logger = env_logger::Builder::from_default_env().build();

        LogWrapper::new(multi.clone(), env_logger);

        let progress_bar = ProgressBar::new((duration / get_update_interval()) as u64);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:90.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
            )
            .unwrap(),
        );

        let pg = multi.add(progress_bar);

        UserInterface {
            logger: CsvLogger::new(file_path),
            progress_bar: pg,
            update_interval: get_update_interval(),
            duration,
            num_sources,
            finished_sources: 0,
        }
    }
    pub fn report_arrived(&mut self, report: Report, cx: &mut Context<Self>) {
        match report {
            Report::PacketSourceReport(PacketSourceReport { timing, .. }) => {
                if timing == ReportTiming::Final {
                    self.finished_sources += 1;
                    debug!(
                        "{} / {} sources have finished.",
                        self.finished_sources, self.num_sources
                    );

                    if self.finished_sources == self.num_sources {
                        self.logger.generate_output_files();

                        self.progress_bar.inc(
                            (self.duration / self.update_interval) as u64
                                - self.progress_bar.position(),
                        );

                        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

                        cx.schedule_event(
                            Duration::from_secs_f64(self.duration - now),
                            Self::run,
                            (),
                        )
                        .unwrap();
                    }
                }

                self.logger.log_report(report, timing);
            }
            Report::PacketSinkReport(PacketSinkReport { timing, .. }) => {
                self.logger.log_report(report, timing);
            }
            Report::SchedulerReport(SchedulerReport { timing, .. }) => {
                self.logger.log_report(report, timing);
            }
        }
    }

    fn run(&mut self, _: (), cx: &mut Context<Self>) {
        if self.finished_sources == self.num_sources {
            self.progress_bar.finish_and_clear();
        } else {
            if self.progress_bar.position() < (self.duration / self.update_interval) as u64 {
                self.progress_bar.inc(1);
            }

            cx.schedule_event(Duration::from_secs_f64(self.update_interval), Self::run, ())
                .unwrap();
        }
    }
}

impl Model for UserInterface {
    async fn init(mut self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        self.run((), cx);
        self.into()
    }
}
