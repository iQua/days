#[cfg(any(feature = "cuda", test))]
mod result_gate {
    use std::fmt;

    use days_executor::RunResult;

    const FNV1A64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV1A64_PRIME: u64 = 0x00000100000001b3;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) struct ResultFingerprint {
        pub(super) serialization_bytes: u64,
        pub(super) fnv1a64: u64,
    }

    struct FingerprintWriter {
        fingerprint: ResultFingerprint,
    }

    impl fmt::Write for FingerprintWriter {
        fn write_str(&mut self, serialization: &str) -> fmt::Result {
            self.fingerprint.serialization_bytes = self
                .fingerprint
                .serialization_bytes
                .checked_add(serialization.len() as u64)
                .ok_or(fmt::Error)?;
            self.fingerprint.fnv1a64 = serialization
                .bytes()
                .fold(self.fingerprint.fnv1a64, |hash, byte| {
                    (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
                });
            Ok(())
        }
    }

    /// Fingerprints the canonical `Debug` serialization of the complete normalized result.
    ///
    /// `RunResult` contains only canonically ordered records, so this serialization is
    /// deterministic and covers every field used by structural equality. Writing directly into
    /// the fingerprint avoids retaining a potentially large second copy of full-mode observations.
    pub(super) fn fingerprint_normalized_result(result: &RunResult) -> ResultFingerprint {
        let mut writer = FingerprintWriter {
            fingerprint: ResultFingerprint {
                serialization_bytes: 0,
                fnv1a64: FNV1A64_OFFSET_BASIS,
            },
        };
        fmt::write(&mut writer, format_args!("{result:#?}"))
            .expect("normalized RunResult serialization length must fit in u64");
        writer.fingerprint
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) struct ResultMismatch {
        scalar_matches_first_cuda: bool,
        scalar_matches_second_cuda: bool,
        cuda_runs_match: bool,
    }

    impl fmt::Display for ResultMismatch {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "normalized result mismatch: scalar_matches_first_cuda={} \
                 scalar_matches_second_cuda={} cuda_runs_match={}",
                self.scalar_matches_first_cuda,
                self.scalar_matches_second_cuda,
                self.cuda_runs_match
            )
        }
    }

    impl std::error::Error for ResultMismatch {}

    pub(super) fn verify_result_equality(
        scalar: &RunResult,
        first_cuda: &RunResult,
        second_cuda: &RunResult,
    ) -> Result<(), ResultMismatch> {
        let mismatch = ResultMismatch {
            scalar_matches_first_cuda: scalar == first_cuda,
            scalar_matches_second_cuda: scalar == second_cuda,
            cuda_runs_match: first_cuda == second_cuda,
        };
        if mismatch.scalar_matches_first_cuda
            && mismatch.scalar_matches_second_cuda
            && mismatch.cuda_runs_match
        {
            Ok(())
        } else {
            Err(mismatch)
        }
    }
}

#[cfg(feature = "cuda")]
mod app {
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor, ObservationMode, run_scalar_with_observations};

    use super::result_gate::{fingerprint_normalized_result, verify_result_equality};

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
        let scalar_fingerprint = fingerprint_normalized_result(&scalar);
        let first_fingerprint = fingerprint_normalized_result(&first.result);
        let second_fingerprint = fingerprint_normalized_result(&second.result);

        println!("fixture={}", fixture.display());
        println!("result_matches_scalar={}", first.result == scalar);
        println!("second_result_matches_scalar={}", second.result == scalar);
        println!("two_run_deterministic={}", first.result == second.result);
        println!(
            "scalar_result_serialization_bytes={}",
            scalar_fingerprint.serialization_bytes
        );
        println!("scalar_result_fnv1a64={:016x}", scalar_fingerprint.fnv1a64);
        println!(
            "first_cuda_result_serialization_bytes={}",
            first_fingerprint.serialization_bytes
        );
        println!(
            "first_cuda_result_fnv1a64={:016x}",
            first_fingerprint.fnv1a64
        );
        println!(
            "second_cuda_result_serialization_bytes={}",
            second_fingerprint.serialization_bytes
        );
        println!(
            "second_cuda_result_fnv1a64={:016x}",
            second_fingerprint.fnv1a64
        );
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
        verify_result_equality(&scalar, &first.result, &second.result)?;
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

#[cfg(test)]
mod tests {
    use days_executor::{RunResult, RunSummary};

    use super::result_gate::{fingerprint_normalized_result, verify_result_equality};

    fn empty_result() -> RunResult {
        RunResult {
            host_states: Vec::new(),
            switch_states: Vec::new(),
            summary: RunSummary::default(),
            resident_packets: Vec::new(),
            observed_packets: Vec::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            pending_events: Vec::new(),
        }
    }

    #[test]
    fn normalized_result_fingerprint_is_stable_and_covers_the_complete_result() {
        let result = empty_result();
        let first = fingerprint_normalized_result(&result);
        let second = fingerprint_normalized_result(&result.clone());

        assert_eq!(first, second);

        let mut changed = result;
        changed.summary.sourced_packets = 1;
        assert_ne!(fingerprint_normalized_result(&changed), first);
    }

    #[test]
    fn result_equality_gate_rejects_each_inequality() {
        let scalar = empty_result();
        let first_cuda = scalar.clone();
        let second_cuda = scalar.clone();
        assert!(verify_result_equality(&scalar, &first_cuda, &second_cuda).is_ok());

        let mut unequal_first = first_cuda.clone();
        unequal_first.summary.sourced_packets = 1;
        assert!(verify_result_equality(&scalar, &unequal_first, &second_cuda).is_err());

        let mut unequal_second = second_cuda;
        unequal_second.summary.received_packets = 1;
        assert!(verify_result_equality(&scalar, &first_cuda, &unequal_second).is_err());
    }
}
