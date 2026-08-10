const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const METAL: &str = include_str!("../src/metal_kernels.metal");
const METAL_HOST: &str = include_str!("../src/metal.rs");

const EVENT_WORDS: usize = 14;
const SCATTER_COOPERATIVE_MIN_RECORDS: usize = 3;

fn kernel_body<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start = source
        .find(start)
        .unwrap_or_else(|| panic!("missing kernel marker `{start}`"));
    let tail = &source[start..];
    let end = tail
        .find(end)
        .unwrap_or_else(|| panic!("missing kernel end marker `{end}`"));
    &tail[..end]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScatterMapping {
    Lane,
    Cooperative,
}

fn scatter_mapping(record_count: usize) -> ScatterMapping {
    if record_count < SCATTER_COOPERATIVE_MIN_RECORDS {
        ScatterMapping::Lane
    } else {
        ScatterMapping::Cooperative
    }
}

#[test]
fn scatter_selects_lane_for_one_cluster_and_cooperative_beyond_it() {
    assert_eq!(EVENT_WORDS * 8, 112, "one record must occupy 112 bytes");
    assert_eq!(2 * EVENT_WORDS, 28, "one cooperative cluster is 28 words");
    assert_eq!(
        2 * EVENT_WORDS * 8,
        224,
        "one cluster must occupy 224 bytes"
    );

    for count in 0..SCATTER_COOPERATIVE_MIN_RECORDS {
        assert_eq!(
            scatter_mapping(count),
            ScatterMapping::Lane,
            "{count} records fit in at most one cooperative cluster"
        );
    }
    for count in SCATTER_COOPERATIVE_MIN_RECORDS..=5 {
        assert_eq!(
            scatter_mapping(count),
            ScatterMapping::Cooperative,
            "{count} records require more than one cooperative cluster"
        );
    }

    for (backend, source) in [("CUDA", CUDA), ("Metal", METAL)] {
        for fragment in [
            "SCATTER_COOPERATIVE_MIN_RECORDS = 3",
            "count < SCATTER_COOPERATIVE_MIN_RECORDS",
            "count >= SCATTER_COOPERATIVE_MIN_RECORDS",
        ] {
            assert!(
                source.contains(fragment),
                "{backend} deterministic scatter selector must contain `{fragment}`"
            );
        }
    }
}

#[test]
fn scatter_keeps_lane_mapping_for_short_and_group_mapping_for_long_producers() {
    let cuda = kernel_body(
        CUDA,
        "__device__ __forceinline__ void scatter_stream_producer",
        "extern \"C\" __global__ void days_exchange_merge",
    );
    for fragment in [
        "SCATTER_GROUP_WIDTH",
        "scatter_stream_producer_lane",
        "worklist[active_index]",
        "__shfl_sync",
        "index += 2",
        "lane < 2 * EVENT_WORDS",
    ] {
        assert!(
            cuda.contains(fragment),
            "CUDA dual-mapping scatter must contain `{fragment}`"
        );
    }

    let metal = kernel_body(
        METAL,
        "kernel void days_exchange_scatter",
        "kernel void days_exchange_merge",
    );
    for fragment in [
        "worklist [[buffer(13)]]",
        "scatter_stream_producer_lane",
        "thread_index_in_simdgroup",
        "simdgroup_index_in_threadgroup",
        "simdgroups_per_threadgroup",
        "scatter_simd_broadcast_ulong",
        "index += 2",
        "lane < 2 * EVENT_WORDS",
    ] {
        assert!(
            metal.contains(fragment),
            "Metal dual-mapping scatter must contain `{fragment}`"
        );
    }
    assert!(
        METAL.contains("simd_broadcast(uint(value), 0)"),
        "Metal must lower each 64-bit cooperative broadcast to supported 32-bit halves"
    );
    assert!(
        METAL_HOST.contains("threadExecutionWidth() < SCATTER_COPY_LANES"),
        "Metal must reject execution widths too narrow for a paired record copy"
    );
    assert!(
        METAL_HOST.contains("round_threads.is_multiple_of(scatter_execution_width)"),
        "Metal must reject threadgroup sizes that leave a partial scatter SIMD group"
    );
}

#[test]
fn scatter_keeps_ordering_leader_owned_and_uses_no_global_sync_trick() {
    for (backend, body) in [
        (
            "CUDA",
            kernel_body(
                CUDA,
                "__device__ __forceinline__ void scatter_stream_producer",
                "extern \"C\" __global__ void days_exchange_merge",
            ),
        ),
        (
            "Metal",
            kernel_body(
                METAL,
                "kernel void days_exchange_scatter",
                "kernel void days_exchange_merge",
            ),
        ),
    ] {
        assert!(
            body.contains("if (lane == 0)"),
            "{backend} leader must own cursor and count mutation"
        );
        for forbidden in ["atomic", "volatile", "threadfence", "memory_order"] {
            assert!(
                !body.contains(forbidden),
                "{backend} scatter must not use forbidden ordering token `{forbidden}`"
            );
        }
    }
}

#[derive(Clone, Copy)]
struct Ring {
    base: usize,
    capacity: usize,
    head: usize,
    count: usize,
    cursor: usize,
}

fn destination(ring: &mut Ring) -> usize {
    let physical = (ring.head + ring.count + ring.cursor) % ring.capacity;
    ring.cursor += 1;
    ring.base + physical
}

fn copy_record(
    source: &[[u64; EVENT_WORDS]],
    source_slot: usize,
    target: &mut [[u64; EVENT_WORDS]],
    target_slot: usize,
) {
    target[target_slot] = source[source_slot];
}

fn scatter_scalar(
    tags: &[usize],
    source: &[[u64; EVENT_WORDS]],
    rings: &mut [Ring],
    target: &mut [[u64; EVENT_WORDS]],
) {
    for (source_slot, channel) in tags.iter().copied().enumerate() {
        let target_slot = destination(&mut rings[channel]);
        copy_record(source, source_slot, target, target_slot);
    }
}

fn scatter_paired(
    tags: &[usize],
    source: &[[u64; EVENT_WORDS]],
    rings: &mut [Ring],
    target: &mut [[u64; EVENT_WORDS]],
) {
    let mut index = 0;
    while index < tags.len() {
        let first = destination(&mut rings[tags[index]]);
        let second = (index + 1 < tags.len()).then(|| destination(&mut rings[tags[index + 1]]));
        copy_record(source, index, target, first);
        if let Some(second) = second {
            copy_record(source, index + 1, target, second);
        }
        index += 2;
    }
}

#[test]
fn paired_copy_preserves_interleaved_channel_slots_and_bytes() {
    let tags = [0, 1, 0, 0, 1];
    let source = std::array::from_fn::<_, 5, _>(|record| {
        std::array::from_fn(|word| ((record as u64) << 32) | word as u64)
    });
    let rings = [
        Ring {
            base: 0,
            capacity: 5,
            head: 3,
            count: 1,
            cursor: 0,
        },
        Ring {
            base: 5,
            capacity: 4,
            head: 2,
            count: 1,
            cursor: 0,
        },
    ];
    let mut scalar_rings = rings;
    let mut paired_rings = rings;
    let mut scalar = [[u64::MAX; EVENT_WORDS]; 9];
    let mut paired = scalar;

    scatter_scalar(&tags, &source, &mut scalar_rings, &mut scalar);
    scatter_paired(&tags, &source, &mut paired_rings, &mut paired);

    assert_eq!(
        paired, scalar,
        "pairing records must preserve every physical ring byte"
    );
    assert_eq!(
        paired_rings
            .iter()
            .map(|ring| ring.cursor)
            .collect::<Vec<_>>(),
        scalar_rings
            .iter()
            .map(|ring| ring.cursor)
            .collect::<Vec<_>>(),
        "pairing records must preserve each channel cursor"
    );
}
