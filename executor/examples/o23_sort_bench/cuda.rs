use std::error::Error;

use cudarc::driver::{
    CudaFunction, CudaGraph, CudaSlice, CudaStream, LaunchConfig, PushKernelArg, sys,
};
use cudarc::nvrtc::Ptx;

use super::{
    BLOCK_THREADS, BenchConfig, Distribution, PASSES, RADIX, TILE_KEYS, emit_protocol, emit_sample,
    make_keys, verify_sorted,
};

const COUNT_NAMES: [&str; PASSES] = [
    "o23_count_0",
    "o23_count_1",
    "o23_count_2",
    "o23_count_3",
    "o23_count_4",
    "o23_count_5",
    "o23_count_6",
    "o23_count_7",
    "o23_count_8",
    "o23_count_9",
    "o23_count_10",
    "o23_count_11",
    "o23_count_12",
    "o23_count_13",
    "o23_count_14",
    "o23_count_15",
];
const SCATTER_NAMES: [&str; PASSES] = [
    "o23_scatter_0",
    "o23_scatter_1",
    "o23_scatter_2",
    "o23_scatter_3",
    "o23_scatter_4",
    "o23_scatter_5",
    "o23_scatter_6",
    "o23_scatter_7",
    "o23_scatter_8",
    "o23_scatter_9",
    "o23_scatter_10",
    "o23_scatter_11",
    "o23_scatter_12",
    "o23_scatter_13",
    "o23_scatter_14",
    "o23_scatter_15",
];

pub(super) fn run(config: &BenchConfig) -> Result<(), Box<dyn Error>> {
    emit_protocol("cuda", 32, config);
    for &distribution in &config.distributions {
        for &width in &config.widths {
            run_case(distribution, width, config)?;
        }
    }
    Ok(())
}

fn run_case(
    distribution: Distribution,
    width: usize,
    config: &BenchConfig,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(width % TILE_KEYS, 0);
    let block_count = u32::try_from(width / TILE_KEYS)?;
    let input = make_keys(width, distribution);

    let context = cudarc::driver::CudaContext::new(0)?;
    let stream = context.new_stream()?;
    unsafe {
        context.disable_event_tracking();
    }
    let (major, minor) = context.compute_capability()?;
    if (major, minor) != (8, 9) && (major, minor) != (12, 1) {
        return Err(format!("O2.3 fatbin does not target sm_{major}{minor}").into());
    }
    let warp_size = context.attribute(sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_WARP_SIZE)?;
    if warp_size != 32 {
        return Err(format!("CUDA device reports warp size {warp_size}, expected 32").into());
    }

    let fatbin = include_bytes!(concat!(env!("OUT_DIR"), "/o23_sort_bench.fatbin"));
    let module = context.load_module(Ptx::from_binary(fatbin.to_vec()))?;
    let count = COUNT_NAMES
        .map(|name| module.load_function(name))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let prefix = module.load_function("o23_prefix")?;
    let scatter = SCATTER_NAMES
        .map(|name| module.load_function(name))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let mut left = stream.clone_htod(&input)?;
    let right = stream.alloc_zeros::<u64>(width)?;
    let ranks = stream.alloc_zeros::<u32>(width)?;
    let histogram_words = usize::try_from(block_count)? * RADIX;
    let block_counts = stream.alloc_zeros::<u32>(histogram_words)?;
    let block_offsets = stream.alloc_zeros::<u32>(histogram_words)?;
    let digit_bases = stream.alloc_zeros::<u32>(RADIX)?;
    stream.synchronize()?;

    let graph = capture_graph(
        &stream,
        block_count,
        &left,
        &right,
        &ranks,
        &block_counts,
        &block_offsets,
        &digit_bases,
        &count,
        &prefix,
        &scatter,
    )?;
    graph.upload()?;
    stream.synchronize()?;

    for sample_index in 0..config.warmups + config.samples {
        stream.memcpy_htod(&input, &mut left)?;
        stream.synchronize()?;

        let start = stream.record_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))?;
        graph.launch()?;
        let end = stream.record_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))?;
        let device_ns = (f64::from(start.elapsed_ms(&end)?) * 1_000_000.0) as u64;

        let output = stream.clone_dtoh(&left)?;
        stream.synchronize()?;
        verify_sorted(&input, &output)?;
        let warmup = sample_index < config.warmups;
        let sample = if warmup {
            format!("warmup{sample_index}")
        } else {
            (sample_index - config.warmups).to_string()
        };
        emit_sample(
            "cuda",
            distribution,
            width,
            &sample,
            warmup,
            device_ns,
            &output,
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn capture_graph(
    stream: &std::sync::Arc<CudaStream>,
    block_count: u32,
    left: &CudaSlice<u64>,
    right: &CudaSlice<u64>,
    ranks: &CudaSlice<u32>,
    block_counts: &CudaSlice<u32>,
    block_offsets: &CudaSlice<u32>,
    digit_bases: &CudaSlice<u32>,
    count: &[CudaFunction],
    prefix: &CudaFunction,
    scatter: &[CudaFunction],
) -> Result<CudaGraph, Box<dyn Error>> {
    let parallel = LaunchConfig {
        grid_dim: (block_count, 1, 1),
        block_dim: (BLOCK_THREADS as u32, 1, 1),
        shared_mem_bytes: 0,
    };
    let single_block = LaunchConfig {
        grid_dim: (1, 1, 1),
        block_dim: (32, 1, 1),
        shared_mem_bytes: 0,
    };

    stream.begin_capture(sys::CUstreamCaptureMode::CU_STREAM_CAPTURE_MODE_THREAD_LOCAL)?;
    let captured = (|| -> Result<(), cudarc::driver::DriverError> {
        for pass in 0..PASSES {
            let (input, output) = if pass.is_multiple_of(2) {
                (left, right)
            } else {
                (right, left)
            };

            let mut args = stream.launch_builder(&count[pass]);
            args.arg(input)
                .arg(ranks)
                .arg(block_counts)
                .arg(&block_count);
            unsafe { args.launch(parallel) }?;

            let mut args = stream.launch_builder(prefix);
            args.arg(block_counts)
                .arg(block_offsets)
                .arg(digit_bases)
                .arg(&block_count);
            unsafe { args.launch(single_block) }?;

            let mut args = stream.launch_builder(&scatter[pass]);
            args.arg(input)
                .arg(output)
                .arg(ranks)
                .arg(block_offsets)
                .arg(digit_bases)
                .arg(&block_count);
            unsafe { args.launch(parallel) }?;
        }
        Ok(())
    })();
    let ended = stream
        .end_capture(sys::CUgraphInstantiate_flags::CUDA_GRAPH_INSTANTIATE_FLAG_USE_NODE_PRIORITY);
    captured?;
    ended?.ok_or_else(|| "CUDA O2.3 graph capture returned no graph".into())
}
