//! Implements a UserInterface struct that includes a progress bar to illustrate the
//! progress of the simulation run. This will eventually evolve to a Ratatui-based
//! user interface that allows real-time interaction with the simulation.

use std::fs;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::time::MonotonicTime;

use crate::flows::FlowFinishMsg;
use days::topos::config::UIConfig;

pub struct UserInterface {
    progress_bar: ProgressBar,
    ui_interval: f64,
    duration: f64,
    num_sources: usize,
    finished_sources: usize,
}

impl UserInterface {
    const RUN_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    pub fn new(num_sources: usize, config_path: &str) -> UserInterface {
        let content = fs::read_to_string(config_path).expect("The configuration is not valid");

        // Obtain the user interface progress interval from the configuration file
        let ui_config: UIConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of the user interface");
        let duration = ui_config.duration.unwrap_or(1.);
        let ui_interval = ui_config.ui_interval.unwrap_or(duration / 100.);

        let multi = MultiProgress::new();
        let env_logger = env_logger::Builder::from_default_env().build();

        LogWrapper::new(multi.clone(), env_logger);

        let progress_bar = ProgressBar::new((duration / ui_interval) as u64);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:90.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
            )
            .unwrap(),
        );

        let pg = multi.add(progress_bar);

        UserInterface {
            progress_bar: pg,
            ui_interval,
            duration,
            num_sources,
            finished_sources: 0,
        }
    }

    pub fn flow_finished(&mut self, _finished: FlowFinishMsg, cx: &Context<Self>) {
        self.finished_sources += 1;
        debug!(
            "{} / {} sources have finished.",
            self.finished_sources, self.num_sources
        );

        if self.finished_sources == self.num_sources {
            self.progress_bar
                .inc((self.duration / self.ui_interval) as u64 - self.progress_bar.position());

            let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            let delay = if self.duration > now {
                self.duration - now
            } else {
                0.0
            };

            if delay <= 0.0 {
                self.run((), cx);
            } else {
                cx.schedule_event_fast(
                    Duration::from_secs_f64(delay),
                    &Self::RUN_SID,
                    Self::run,
                    (),
                )
                .unwrap();
            }
        }
    }

    fn run(&mut self, _: (), cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        if now >= self.duration {
            self.progress_bar.finish_and_clear();
        } else {
            if self.progress_bar.position() < (self.duration / self.ui_interval) as u64 {
                self.progress_bar.inc(1);
            }

            cx.schedule_event_fast(
                Duration::from_secs_f64(self.ui_interval),
                &Self::RUN_SID,
                Self::run,
                (),
            )
            .unwrap();
        }
    }
}

impl Model for UserInterface {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::run));
        registry
    }

    async fn init(mut self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        self.run((), cx);
        self.into()
    }
}
