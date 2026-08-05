#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
mod app {
    use std::path::PathBuf;

    use clap::{Parser, ValueEnum};
    use days::scenario::compile_config;
    #[cfg(feature = "cuda-test-hooks")]
    use days_executor::{CudaConfig, measure_cuda_planner_for_testing};
    use days_executor::{DeviceCapacityCaps, ObservationMode};
    #[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
    use days_executor::{MetalConfig, measure_metal_planner_for_testing};

    const CAPACITY_CAPS: DeviceCapacityCaps = DeviceCapacityCaps {
        fallback_fel_events_per_lp: Some(16_384),
        queue_packets_per_lp: Some(2_048),
        channel_events_per_stream: Some(2_048),
        remote_staging_events_per_lp: Some(2_048),
        outbox_events_total: Some(2_000_000),
        tcp_receiver_ranges_per_flow: Some(64),
        tcp_ledger_segments_per_flow: Some(4_096),
        observation_events_per_lp: Some(512),
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
    enum Backend {
        #[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
        Metal,
        #[cfg(feature = "cuda-test-hooks")]
        Cuda,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
    enum Planner {
        Precomputed,
        Legacy,
    }

    #[derive(Debug, Parser)]
    #[command(about = "Measure T20e host-side device plan construction only")]
    struct Cli {
        fixture: PathBuf,
        #[arg(long, value_enum)]
        backend: Backend,
        #[arg(long, value_enum, default_value = "precomputed")]
        planner: Planner,
        #[arg(long, default_value_t = 1)]
        samples: usize,
    }

    pub fn run() -> Result<(), String> {
        let cli = Cli::parse();
        if cli.samples == 0 {
            return Err("--samples must be nonzero".into());
        }
        let image = compile_config(&cli.fixture)
            .map_err(|error| format!("failed to lower {}: {error}", cli.fixture.display()))?;
        let legacy = cli.planner == Planner::Legacy;
        for sample in 1..=cli.samples {
            let planning_ns = match cli.backend {
                #[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
                Backend::Metal => measure_metal_planner_for_testing(
                    &image,
                    None,
                    MetalConfig {
                        capacity_caps: CAPACITY_CAPS,
                        ..MetalConfig::default()
                    },
                    ObservationMode::Summary,
                    legacy,
                )
                .map_err(|error| error.to_string())?,
                #[cfg(feature = "cuda-test-hooks")]
                Backend::Cuda => measure_cuda_planner_for_testing(
                    &image,
                    None,
                    CudaConfig {
                        capacity_caps: CAPACITY_CAPS,
                        ..CudaConfig::default()
                    },
                    ObservationMode::Summary,
                    legacy,
                )
                .map_err(|error| error.to_string())?,
            };
            println!(
                "fixture={} backend={:?} planner={:?} sample={} planning_ns={} planning_seconds={:.9}",
                cli.fixture.display(),
                cli.backend,
                cli.planner,
                sample,
                planning_ns,
                planning_ns as f64 / 1_000_000_000.0
            );
        }
        Ok(())
    }
}

#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
fn main() {
    if let Err(error) = app::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(not(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
)))]
fn main() {
    eprintln!("enable metal-test-hooks on Apple or cuda-test-hooks to benchmark a device planner");
    std::process::exit(2);
}
