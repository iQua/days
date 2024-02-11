use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::{debug, info};
use std::time::Duration;

fn main() {
    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();
    let multi = MultiProgress::new();

    LogWrapper::new(multi.clone(), logger).try_init().unwrap();

    let progress_bar = ProgressBar::new(5);
    progress_bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("#-"),
    );

    let pg = multi.add(progress_bar);
    for i in 0..5 {
        std::thread::sleep(Duration::from_secs(1));
        info!("iteration {}", i);
        debug!("iteration {}", i);
        pg.inc(1);
    }
}
