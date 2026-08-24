//! Implements a UserInterface struct that includes a progress bar to illustrate the
//! progress of the simulation run. This will eventually evolve to a Ratatui-based
//! user interface that allows real-time interaction with the simulation.

use std::fs;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::debug;

use crate::flows::FlowFinishMsg;
use crate::utils::exact_time::{clock_ns, scenario_seconds_ns};
use days::topos::config::UIConfig;
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};

pub struct UserInterface {
    progress_bar: ProgressBar,
    ui_interval_ns: u64,
    duration_ns: u64,
    num_sources: usize,
    finished_sources: usize,
}

impl UserInterface {
    const RUN_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    pub fn new(num_sources: usize, config_path: &str) -> UserInterface {
        crate::validate_config(config_path).unwrap_or_else(|error| panic!("{error}"));
        let content = fs::read_to_string(config_path).expect("The configuration is not valid");

        // Obtain the user interface progress interval from the configuration file
        let ui_config: UIConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of the user interface");
        let duration_ns = scenario_seconds_ns(ui_config.duration.unwrap_or(1.0), "UI duration")
            .unwrap_or_else(|error| panic!("{error}"));
        let ui_interval_ns = match ui_config.ui_interval {
            Some(interval) => scenario_seconds_ns(interval, "UI interval")
                .unwrap_or_else(|error| panic!("{error}")),
            None => (duration_ns / 100).max(1),
        };

        let multi = MultiProgress::new();
        let env_logger = env_logger::Builder::from_default_env().build();

        LogWrapper::new(multi.clone(), env_logger);

        let progress_bar = ProgressBar::new(duration_ns / ui_interval_ns);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:90.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
            )
            .unwrap(),
        );

        let pg = multi.add(progress_bar);

        UserInterface {
            progress_bar: pg,
            ui_interval_ns,
            duration_ns,
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
                .inc(self.duration_ns / self.ui_interval_ns - self.progress_bar.position());

            let now_ns = clock_ns(cx.time());
            let delay_ns = self.duration_ns.saturating_sub(now_ns);

            if delay_ns == 0 {
                self.run((), cx);
            } else {
                cx.schedule_event_fast(
                    Duration::from_nanos(delay_ns),
                    &Self::RUN_SID,
                    Self::run,
                    (),
                )
                .unwrap();
            }
        }
    }

    fn run(&mut self, _: (), cx: &Context<Self>) {
        let now_ns = clock_ns(cx.time());

        if now_ns >= self.duration_ns {
            self.progress_bar.finish_and_clear();
        } else {
            if self.progress_bar.position() < self.duration_ns / self.ui_interval_ns {
                self.progress_bar.inc(1);
            }

            cx.schedule_event_fast(
                Duration::from_nanos(self.ui_interval_ns),
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
