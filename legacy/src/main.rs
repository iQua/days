//! The main program for running a simulation using a specific configuration.

use std::env;

use log::info;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use days::utils::tracing::{ConcurrencyTrackerLayer, is_tracing_active};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Please provide the path to the toml configuration file: cargo run -- <path>");
    }

    let path = args[1].clone();

    if let Err(error) = days_legacy::validate_config(&path) {
        eprintln!("Simulation failed: {error}");
        std::process::exit(1);
    }

    // builds an EnvFilter that reads the RUST_LOG environment variable, defaulting to `info` if
    // not set

    if is_tracing_active(&path) {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        tracing_subscriber::registry()
            .with(fmt::layer()) // console formatting
            .with(env_filter) // env-based filtering
            .with(ConcurrencyTrackerLayer) // concurrency tracking
            .init();

        info!("Concurrency tracing is active.");
    } else {
        let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
        env_logger::init_from_env(env);
    }

    if let Err(error) = days_legacy::run_simulation_from_config(&path) {
        eprintln!("Simulation failed: {error}");
        std::process::exit(1);
    }
}
