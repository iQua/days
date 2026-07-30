#[cfg(feature = "cuda")]
mod app {
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor, ObservationMode, run_scalar_with_observations};

    const DEFAULT_FIXTURE: &str = "configs/benchmarks/baseline/fattree_k4_f8_st.toml";

    pub fn run() -> Result<(), Box<dyn Error>> {
        let fixture = std::env::args_os()
            .nth(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_FIXTURE));
        let image = compile_config(Path::new(&fixture))?;
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)?;
        let initialized = Instant::now();
        let executor = CudaExecutor::new()?;
        let initialization_wall_ns = elapsed_ns(initialized);
        let initialization = executor.initialization_timings();

        let first_started = Instant::now();
        let first = executor.run_with_observations(
            &image,
            None,
            CudaConfig::default(),
            ObservationMode::Full,
        )?;
        let first_end_to_end_wall_ns = elapsed_ns(first_started);
        let second_started = Instant::now();
        let second = executor.run_with_observations(
            &image,
            None,
            CudaConfig::default(),
            ObservationMode::Full,
        )?;
        let second_end_to_end_wall_ns = elapsed_ns(second_started);

        println!("fixture={}", fixture.display());
        println!("result_matches_scalar={}", first.result == scalar);
        println!("two_run_deterministic={}", first.result == second.result);
        println!("rounds={}", first.rounds);
        println!("transitions={}", first.transitions);
        println!("encoded_attempts={}", first.encoded_attempts);
        println!("graph_replays={}", first.graph_replays);
        println!("wave_boundary_syncs={}", first.wave_boundary_syncs);
        println!(
            "mid_round_wave_boundary_syncs={}",
            first.mid_round_wave_boundary_syncs
        );
        println!("initialization_wall_ns={initialization_wall_ns}");
        println!(
            "context_stream_setup_ns={}",
            initialization.context_stream_setup_ns
        );
        println!(
            "module_function_load_ns={}",
            initialization.module_function_load_ns
        );
        println!("first_graph_capture_ns={}", first.graph_capture_ns);
        println!("first_host_submit_ns={}", first.host_submit_ns);
        println!("first_device_ns={}", first.device_ns);
        println!("first_graph_wall_ns={}", first.wall_ns);
        println!("first_end_to_end_wall_ns={first_end_to_end_wall_ns}");
        println!("second_graph_capture_ns={}", second.graph_capture_ns);
        println!("second_host_submit_ns={}", second.host_submit_ns);
        println!("second_device_ns={}", second.device_ns);
        println!("second_graph_wall_ns={}", second.wall_ns);
        println!("second_end_to_end_wall_ns={second_end_to_end_wall_ns}");
        Ok(())
    }

    fn elapsed_ns(started: Instant) -> u64 {
        started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

#[cfg(feature = "cuda")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::run()
}

#[cfg(not(feature = "cuda"))]
fn main() {
    panic!("t16_cuda_gate requires --features cuda");
}
