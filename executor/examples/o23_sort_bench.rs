#![cfg_attr(test, allow(dead_code))]

const WIDTHS: [usize; 2] = [49_152, 147_456];
const DEFAULT_SAMPLES: usize = 5;
const BLOCK_THREADS: usize = 256;
const PASSES: usize = 16;
const RADIX: usize = 16;
const ITEMS_PER_THREAD: usize = 4;
const TILE_KEYS: usize = BLOCK_THREADS * ITEMS_PER_THREAD;
const SEED: u64 = 0x6f32_335f_736f_7274;
const ALL_EQUAL_KEY: u64 = 0xa11e_9a1e_d15e_a5ed;
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Distribution {
    Uniform,
    AllEqual,
}

#[derive(Debug, Eq, PartialEq)]
struct BenchConfig {
    widths: Vec<usize>,
    distributions: Vec<Distribution>,
    samples: usize,
    warmups: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            widths: WIDTHS.to_vec(),
            distributions: Distribution::ALL.to_vec(),
            samples: DEFAULT_SAMPLES,
            warmups: 1,
        }
    }
}

impl BenchConfig {
    fn parse<I, S>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| format!("{} requires a value", flag.as_ref()))?;
            match flag.as_ref() {
                "--width" => {
                    let width = value
                        .as_ref()
                        .parse::<usize>()
                        .map_err(|error| format!("invalid --width: {error}"))?;
                    if !WIDTHS.contains(&width) {
                        return Err(format!("unsupported --width {width}"));
                    }
                    config.widths = vec![width];
                }
                "--distribution" => {
                    let distribution = match value.as_ref() {
                        "uniform" => Distribution::Uniform,
                        "all_equal" => Distribution::AllEqual,
                        other => return Err(format!("unsupported --distribution {other}")),
                    };
                    config.distributions = vec![distribution];
                }
                "--samples" => {
                    config.samples = value
                        .as_ref()
                        .parse()
                        .map_err(|error| format!("invalid --samples: {error}"))?;
                }
                "--warmups" => {
                    config.warmups = value
                        .as_ref()
                        .parse()
                        .map_err(|error| format!("invalid --warmups: {error}"))?;
                }
                other => return Err(format!("unsupported argument {other}")),
            }
        }
        if config.samples == 0 && config.warmups == 0 {
            return Err("at least one sample or warm-up is required".into());
        }
        Ok(config)
    }
}

impl Distribution {
    const ALL: [Self; 2] = [Self::Uniform, Self::AllEqual];

    fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::AllEqual => "all_equal",
        }
    }
}

fn make_keys(width: usize, distribution: Distribution) -> Vec<u64> {
    match distribution {
        Distribution::Uniform => {
            let mut state = SEED;
            (0..width)
                .map(|_| {
                    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                    let mut value = state;
                    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                    value ^ (value >> 31)
                })
                .collect()
        }
        Distribution::AllEqual => vec![ALL_EQUAL_KEY; width],
    }
}

fn verify_sorted(input: &[u64], output: &[u64]) -> Result<(), String> {
    if output.len() != input.len() {
        return Err(format!(
            "output length {} differs from input length {}",
            output.len(),
            input.len()
        ));
    }
    if let Some((index, pair)) = output
        .windows(2)
        .enumerate()
        .find(|(_, pair)| pair[0] > pair[1])
    {
        return Err(format!(
            "output is not sorted at {index}: {} > {}",
            pair[0], pair[1]
        ));
    }
    let mut expected = input.to_vec();
    expected.sort_unstable();
    if expected != output {
        return Err("output differs from the exact CPU reference".into());
    }
    Ok(())
}

fn fnv1a64(values: &[u64]) -> u64 {
    values.iter().fold(FNV1A64_OFFSET_BASIS, |hash, value| {
        value.to_le_bytes().into_iter().fold(hash, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
        })
    })
}

fn emit_protocol(backend: &str, simd_width: usize, config: &BenchConfig) {
    let widths = config
        .widths
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let distributions = config
        .distributions
        .iter()
        .map(|distribution| distribution.name())
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "record=o23_sort_protocol backend={backend} algorithm=stable_lsd_radix16 \
         passes={PASSES} block_threads={BLOCK_THREADS} simd_width={simd_width} \
         prefix=deterministic_block_then_digit widths={widths} distributions={distributions} \
         seed={SEED:016x} samples={} warmups={} clock=device_ns input_reset=outside_clock",
        config.samples, config.warmups,
    );
}

fn emit_sample(
    backend: &str,
    distribution: Distribution,
    width: usize,
    sample: &str,
    warmup: bool,
    device_ns: u64,
    output: &[u64],
) {
    println!(
        "record=o23_sort_sample backend={backend} distribution={} width={width} sample={sample} \
         warmup={} device_ns={device_ns} total_us={:.3} ns_per_key={:.6} \
         output_bytes={} output_fnv1a64={:016x} sorted=1 exact_cpu_match=1",
        distribution.name(),
        u8::from(warmup),
        device_ns as f64 / 1_000.0,
        device_ns as f64 / width as f64,
        std::mem::size_of_val(output),
        fnv1a64(output),
    );
}

#[cfg(all(feature = "o23-sort-bench", feature = "cuda"))]
#[path = "o23_sort_bench/cuda.rs"]
mod cuda;

#[cfg(all(
    feature = "o23-sort-bench",
    feature = "metal-spike",
    target_vendor = "apple"
))]
#[path = "o23_sort_bench/metal.rs"]
mod metal;

#[cfg(all(
    not(test),
    feature = "o23-sort-bench",
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
fn main() {
    let config = BenchConfig::parse(std::env::args().skip(1))
        .expect("O2.3 sort benchmark arguments must be valid");
    cuda::run(&config).expect("CUDA O2.3 sort benchmark must succeed");
}

#[cfg(all(
    not(test),
    feature = "o23-sort-bench",
    feature = "metal-spike",
    target_vendor = "apple"
))]
fn main() {
    let config = BenchConfig::parse(std::env::args().skip(1))
        .expect("O2.3 sort benchmark arguments must be valid");
    metal::run(&config).expect("Metal O2.3 sort benchmark must succeed");
}

#[cfg(all(
    not(test),
    not(all(feature = "o23-sort-bench", feature = "cuda")),
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
fn main() {
    eprintln!("o23_sort_bench requires --features cuda or Apple --features metal-spike");
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::{BenchConfig, Distribution, WIDTHS, make_keys, verify_sorted};

    #[test]
    fn command_line_can_isolate_one_quiet_gated_transaction() {
        let config = BenchConfig::parse([
            "--width",
            "49152",
            "--distribution",
            "uniform",
            "--samples",
            "1",
            "--warmups",
            "0",
        ])
        .expect("single-transaction arguments must parse");

        assert_eq!(config.widths, vec![49_152]);
        assert_eq!(config.distributions, vec![Distribution::Uniform]);
        assert_eq!(config.samples, 1);
        assert_eq!(config.warmups, 0);
    }

    #[test]
    fn fixed_seed_inputs_are_reproducible() {
        for width in WIDTHS {
            assert_eq!(
                make_keys(width, Distribution::Uniform),
                make_keys(width, Distribution::Uniform)
            );
            assert_eq!(
                make_keys(width, Distribution::AllEqual),
                make_keys(width, Distribution::AllEqual)
            );
        }
    }

    #[test]
    fn distributions_cover_uniform_and_all_equal_controls() {
        for width in WIDTHS {
            let uniform = make_keys(width, Distribution::Uniform);
            assert!(uniform.windows(2).any(|pair| pair[0] != pair[1]));

            let equal = make_keys(width, Distribution::AllEqual);
            assert!(equal.windows(2).all(|pair| pair[0] == pair[1]));
        }
    }

    #[test]
    fn sorted_output_must_match_the_exact_cpu_reference() {
        let input = make_keys(256, Distribution::Uniform);
        let mut expected = input.clone();
        expected.sort();
        verify_sorted(&input, &expected).expect("the CPU reference must pass");

        let mut corrupted = expected;
        corrupted.swap(0, 1);
        corrupted[0] ^= 1;
        assert!(verify_sorted(&input, &corrupted).is_err());
    }
}
