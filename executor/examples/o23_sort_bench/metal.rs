use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::NSString;
use objc2_metal::{
    MTLBarrierScope, MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
    MTLCommandQueue, MTLComputeCommandEncoder, MTLComputePipelineState,
    MTLCreateSystemDefaultDevice, MTLDevice, MTLDispatchType, MTLLibrary, MTLResourceOptions,
    MTLSize,
};

use super::{
    BLOCK_THREADS, BenchConfig, Distribution, PASSES, RADIX, TILE_KEYS, emit_protocol, emit_sample,
    make_keys, verify_sorted,
};

type RawBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type Pipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

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

struct Buffer {
    raw: RawBuffer,
    bytes: usize,
}

impl Buffer {
    fn zeroed(device: &ProtocolObject<dyn MTLDevice>, bytes: usize) -> Result<Self, String> {
        let raw = device
            .newBufferWithLength_options(bytes.max(1), MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| format!("failed to allocate a {}-byte Metal buffer", bytes.max(1)))?;
        unsafe {
            std::ptr::write_bytes(raw.contents().as_ptr(), 0, bytes.max(1));
        }
        Ok(Self { raw, bytes })
    }

    fn write_u64(&self, values: &[u64]) {
        assert_eq!(self.bytes, std::mem::size_of_val(values));
        unsafe {
            std::ptr::copy_nonoverlapping(
                values.as_ptr(),
                self.raw.contents().as_ptr().cast::<u64>(),
                values.len(),
            );
        }
    }

    fn write_u32(&self, values: &[u32]) {
        assert_eq!(self.bytes, std::mem::size_of_val(values));
        unsafe {
            std::ptr::copy_nonoverlapping(
                values.as_ptr(),
                self.raw.contents().as_ptr().cast::<u32>(),
                values.len(),
            );
        }
    }

    fn read_u64(&self) -> Vec<u64> {
        let len = self.bytes / std::mem::size_of::<u64>();
        unsafe {
            std::slice::from_raw_parts(self.raw.contents().as_ptr().cast::<u64>(), len).to_vec()
        }
    }

    fn read_u32(&self) -> Vec<u32> {
        let len = self.bytes / std::mem::size_of::<u32>();
        unsafe {
            std::slice::from_raw_parts(self.raw.contents().as_ptr().cast::<u32>(), len).to_vec()
        }
    }
}

struct MetalSort {
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    count: Vec<Pipeline>,
    prefix: Pipeline,
    scatter: Vec<Pipeline>,
    simd_probe: Pipeline,
}

impl MetalSort {
    fn new() -> Result<Self, String> {
        let device = MTLCreateSystemDefaultDevice()
            .ok_or_else(|| "no system-default Metal device is available".to_owned())?;
        let queue = device
            .newCommandQueue()
            .ok_or_else(|| "Metal command queue creation failed".to_owned())?;
        let source = NSString::from_str(include_str!("../../src/o23_sort_bench.metal"));
        let library = device
            .newLibraryWithSource_options_error(&source, None)
            .map_err(|error| {
                format!(
                    "O2.3 MSL compilation failed: {}",
                    error.localizedDescription()
                )
            })?;
        let count = COUNT_NAMES
            .iter()
            .map(|name| create_pipeline(&device, &library, name))
            .collect::<Result<Vec<_>, _>>()?;
        let prefix = create_pipeline(&device, &library, "o23_prefix")?;
        let scatter = SCATTER_NAMES
            .iter()
            .map(|name| create_pipeline(&device, &library, name))
            .collect::<Result<Vec<_>, _>>()?;
        let simd_probe = create_pipeline(&device, &library, "o23_simd_probe")?;

        for (name, pipeline) in COUNT_NAMES
            .iter()
            .chain(["o23_prefix"].iter())
            .chain(SCATTER_NAMES.iter())
            .zip(count.iter().chain([&prefix]).chain(scatter.iter()))
        {
            if pipeline.threadExecutionWidth() != 32 {
                return Err(format!(
                    "Metal pipeline {name} reports SIMD width {}, expected 32",
                    pipeline.threadExecutionWidth()
                ));
            }
            let required = if *name == "o23_prefix" {
                32
            } else {
                BLOCK_THREADS
            };
            if pipeline.maxTotalThreadsPerThreadgroup() < required {
                return Err(format!(
                    "Metal pipeline {name} supports only {} threads, expected at least {required}",
                    pipeline.maxTotalThreadsPerThreadgroup()
                ));
            }
        }

        Ok(Self {
            device,
            queue,
            count,
            prefix,
            scatter,
            simd_probe,
        })
    }

    fn validate_simd_rank(&self) -> Result<(), String> {
        let digits = (0..BLOCK_THREADS)
            .map(|index| (index % RADIX) as u32)
            .collect::<Vec<_>>();
        let input = Buffer::zeroed(&self.device, std::mem::size_of_val(digits.as_slice()))?;
        input.write_u32(&digits);
        let ranks = Buffer::zeroed(&self.device, BLOCK_THREADS * std::mem::size_of::<u32>())?;
        let geometry =
            Buffer::zeroed(&self.device, BLOCK_THREADS * 2 * std::mem::size_of::<u32>())?;
        let command_buffer = self
            .queue
            .commandBuffer()
            .ok_or_else(|| "Metal SIMD probe command buffer creation failed".to_owned())?;
        let encoder = command_buffer
            .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
            .ok_or_else(|| "Metal SIMD probe encoder creation failed".to_owned())?;
        encoder.setComputePipelineState(&self.simd_probe);
        bind(&encoder, 0, &input);
        bind(&encoder, 1, &ranks);
        bind(&encoder, 2, &geometry);
        encoder.dispatchThreadgroups_threadsPerThreadgroup(
            MTLSize {
                width: 1,
                height: 1,
                depth: 1,
            },
            MTLSize {
                width: BLOCK_THREADS,
                height: 1,
                depth: 1,
            },
        );
        encoder.endEncoding();
        command_buffer.commit();
        command_buffer.waitUntilCompleted();
        let actual_ranks = ranks.read_u32();
        let actual_geometry = geometry.read_u32();
        for index in 0..BLOCK_THREADS {
            let expected_rank = (index / RADIX) as u32;
            let expected_lane = (index % 32) as u32;
            let expected_group = (index / 32) as u32;
            if actual_ranks[index] != expected_rank
                || actual_geometry[index * 2] != expected_lane
                || actual_geometry[index * 2 + 1] != expected_group
            {
                return Err(format!(
                    "Metal SIMD probe mismatch at {index}: rank/lane/group={}/{}/{} expected \
                     {expected_rank}/{expected_lane}/{expected_group}",
                    actual_ranks[index],
                    actual_geometry[index * 2],
                    actual_geometry[index * 2 + 1],
                ));
            }
        }
        Ok(())
    }

    fn run_case(
        &self,
        distribution: Distribution,
        width: usize,
        config: &BenchConfig,
    ) -> Result<(), String> {
        assert_eq!(width % TILE_KEYS, 0);
        let block_count = u32::try_from(width / TILE_KEYS)
            .map_err(|_| "O2.3 block count does not fit u32".to_owned())?;
        let input = make_keys(width, distribution);
        let key_bytes = width * std::mem::size_of::<u64>();
        let histogram_bytes = block_count as usize * RADIX * std::mem::size_of::<u32>();
        let left = Buffer::zeroed(&self.device, key_bytes)?;
        let right = Buffer::zeroed(&self.device, key_bytes)?;
        let ranks = Buffer::zeroed(&self.device, width * std::mem::size_of::<u32>())?;
        let block_counts = Buffer::zeroed(&self.device, histogram_bytes)?;
        let block_offsets = Buffer::zeroed(&self.device, histogram_bytes)?;
        let digit_bases = Buffer::zeroed(&self.device, RADIX * std::mem::size_of::<u32>())?;
        let blocks = Buffer::zeroed(&self.device, std::mem::size_of::<u32>())?;
        blocks.write_u32(&[block_count]);

        for sample_index in 0..config.warmups + config.samples {
            left.write_u64(&input);
            let command_buffer = self
                .queue
                .commandBuffer()
                .ok_or_else(|| "Metal O2.3 command buffer creation failed".to_owned())?;
            let encoder = command_buffer
                .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
                .ok_or_else(|| "Metal O2.3 compute encoder creation failed".to_owned())?;
            let parallel_grid = MTLSize {
                width: block_count as usize,
                height: 1,
                depth: 1,
            };
            let parallel_group = MTLSize {
                width: BLOCK_THREADS,
                height: 1,
                depth: 1,
            };
            let prefix_grid = MTLSize {
                width: 1,
                height: 1,
                depth: 1,
            };
            let prefix_group = MTLSize {
                width: 32,
                height: 1,
                depth: 1,
            };

            for pass in 0..PASSES {
                let (input_buffer, output_buffer) = if pass.is_multiple_of(2) {
                    (&left, &right)
                } else {
                    (&right, &left)
                };

                encoder.setComputePipelineState(&self.count[pass]);
                bind(&encoder, 0, input_buffer);
                bind(&encoder, 1, &ranks);
                bind(&encoder, 2, &block_counts);
                bind(&encoder, 3, &blocks);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(parallel_grid, parallel_group);
                encoder.memoryBarrierWithScope(MTLBarrierScope::Buffers);

                encoder.setComputePipelineState(&self.prefix);
                bind(&encoder, 0, &block_counts);
                bind(&encoder, 1, &block_offsets);
                bind(&encoder, 2, &digit_bases);
                bind(&encoder, 3, &blocks);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(prefix_grid, prefix_group);
                encoder.memoryBarrierWithScope(MTLBarrierScope::Buffers);

                encoder.setComputePipelineState(&self.scatter[pass]);
                bind(&encoder, 0, input_buffer);
                bind(&encoder, 1, output_buffer);
                bind(&encoder, 2, &ranks);
                bind(&encoder, 3, &block_offsets);
                bind(&encoder, 4, &digit_bases);
                bind(&encoder, 5, &blocks);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(parallel_grid, parallel_group);
                encoder.memoryBarrierWithScope(MTLBarrierScope::Buffers);
            }
            encoder.endEncoding();
            command_buffer.commit();
            command_buffer.waitUntilCompleted();
            if command_buffer.status() != MTLCommandBufferStatus::Completed {
                let detail = command_buffer
                    .error()
                    .map(|error| error.localizedDescription().to_string())
                    .unwrap_or_else(|| "no NSError detail".into());
                return Err(format!(
                    "Metal O2.3 command buffer status {:?}: {detail}",
                    command_buffer.status()
                ));
            }
            let start = command_buffer.GPUStartTime();
            let end = command_buffer.GPUEndTime();
            if !start.is_finite() || !end.is_finite() || end < start {
                return Err(format!("invalid Metal GPU timestamp range {start}..{end}"));
            }
            let device_ns = std::time::Duration::from_secs_f64(end - start)
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            let output = left.read_u64();
            verify_sorted(&input, &output)?;
            let warmup = sample_index < config.warmups;
            let sample = if warmup {
                format!("warmup{sample_index}")
            } else {
                (sample_index - config.warmups).to_string()
            };
            emit_sample(
                "metal",
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
}

pub(super) fn run(config: &BenchConfig) -> Result<(), String> {
    let sort = MetalSort::new()?;
    sort.validate_simd_rank()?;
    emit_protocol("metal", 32, config);
    for &distribution in &config.distributions {
        for &width in &config.widths {
            sort.run_case(distribution, width, config)?;
        }
    }
    Ok(())
}

fn create_pipeline(
    device: &ProtocolObject<dyn MTLDevice>,
    library: &ProtocolObject<dyn MTLLibrary>,
    name: &str,
) -> Result<Pipeline, String> {
    let function_name = NSString::from_str(name);
    let function = library
        .newFunctionWithName(&function_name)
        .ok_or_else(|| format!("O2.3 MSL entry point not found: {name}"))?;
    device
        .newComputePipelineStateWithFunction_error(&function)
        .map_err(|error| {
            format!(
                "O2.3 pipeline creation failed for {name}: {}",
                error.localizedDescription()
            )
        })
}

fn bind(encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>, index: usize, buffer: &Buffer) {
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(&buffer.raw), 0, index);
    }
}
