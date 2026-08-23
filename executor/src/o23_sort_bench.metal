#include <metal_stdlib>

using namespace metal;

constant uint O23_BLOCK_THREADS = 256;
constant uint O23_SIMD_GROUPS = 8;
constant uint O23_ITEMS_PER_THREAD = 4;
constant uint O23_TILE_KEYS = O23_BLOCK_THREADS * O23_ITEMS_PER_THREAD;
constant uint O23_RADIX = 16;

#define O23_COUNT(PASS, SHIFT)                                                         \
    kernel void o23_count_##PASS(                                                      \
        device const ulong* keys [[buffer(0)]],                                        \
        device uint* ranks [[buffer(1)]],                                              \
        device uint* block_counts [[buffer(2)]],                                       \
        constant uint& block_count [[buffer(3)]],                                      \
        uint tid [[thread_position_in_threadgroup]],                                   \
        uint block [[threadgroup_position_in_grid]],                                   \
        uint simd_lane [[thread_index_in_simdgroup]],                                  \
        uint simd_group [[simdgroup_index_in_threadgroup]]) {                          \
        threadgroup uint simd_counts[8 * 16];                                          \
        threadgroup uint seen[16];                                                     \
        const uint tile_base = block * O23_TILE_KEYS;                                  \
        if (tid < O23_RADIX) {                                                         \
            seen[tid] = 0;                                                            \
        }                                                                              \
        threadgroup_barrier(mem_flags::mem_threadgroup);                              \
        for (uint item = 0; item < O23_ITEMS_PER_THREAD; ++item) {                    \
            const uint index = tile_base + item * O23_BLOCK_THREADS + tid;            \
            const uint digit = uint((keys[index] >> SHIFT) & 0xful);                  \
            uint within_simd = 0;                                                     \
            for (uint current = 0; current < O23_RADIX; ++current) {                  \
                const uint present = uint(digit == current);                          \
                const uint prefix = simd_prefix_exclusive_sum(present);               \
                const uint total = simd_sum(present);                                 \
                if (simd_lane == current) {                                           \
                    simd_counts[simd_group * O23_RADIX + current] = total;            \
                }                                                                      \
                if (digit == current) {                                               \
                    within_simd = prefix;                                             \
                }                                                                      \
            }                                                                          \
            threadgroup_barrier(mem_flags::mem_threadgroup);                          \
            uint local_rank = seen[digit] + within_simd;                              \
            for (uint group = 0; group < simd_group; ++group) {                       \
                local_rank += simd_counts[group * O23_RADIX + digit];                 \
            }                                                                          \
            ranks[index] = local_rank;                                                 \
            threadgroup_barrier(mem_flags::mem_threadgroup);                          \
            if (tid < O23_RADIX) {                                                    \
                uint item_count = 0;                                                  \
                for (uint group = 0; group < O23_SIMD_GROUPS; ++group) {              \
                    item_count += simd_counts[group * O23_RADIX + tid];               \
                }                                                                      \
                seen[tid] += item_count;                                              \
            }                                                                          \
            threadgroup_barrier(mem_flags::mem_threadgroup);                          \
        }                                                                              \
        if (tid < O23_RADIX) {                                                        \
            block_counts[tid * block_count + block] = seen[tid];                     \
        }                                                                              \
    }

#define O23_SCATTER(PASS, SHIFT)                                                       \
    kernel void o23_scatter_##PASS(                                                    \
        device const ulong* input [[buffer(0)]],                                       \
        device ulong* output [[buffer(1)]],                                            \
        device const uint* ranks [[buffer(2)]],                                        \
        device const uint* block_offsets [[buffer(3)]],                                \
        device const uint* digit_bases [[buffer(4)]],                                  \
        constant uint& block_count [[buffer(5)]],                                      \
        uint tid [[thread_position_in_threadgroup]],                                   \
        uint block [[threadgroup_position_in_grid]]) {                                 \
        const uint tile_base = block * O23_TILE_KEYS;                                  \
        for (uint item = 0; item < O23_ITEMS_PER_THREAD; ++item) {                    \
            const uint index = tile_base + item * O23_BLOCK_THREADS + tid;            \
            const ulong key = input[index];                                            \
            const uint digit = uint((key >> SHIFT) & 0xful);                          \
            const uint destination = digit_bases[digit]                               \
                + block_offsets[digit * block_count + block] + ranks[index];          \
            output[destination] = key;                                                 \
        }                                                                              \
    }

O23_COUNT(0, 0)
O23_COUNT(1, 4)
O23_COUNT(2, 8)
O23_COUNT(3, 12)
O23_COUNT(4, 16)
O23_COUNT(5, 20)
O23_COUNT(6, 24)
O23_COUNT(7, 28)
O23_COUNT(8, 32)
O23_COUNT(9, 36)
O23_COUNT(10, 40)
O23_COUNT(11, 44)
O23_COUNT(12, 48)
O23_COUNT(13, 52)
O23_COUNT(14, 56)
O23_COUNT(15, 60)

kernel void o23_prefix(
    device const uint* block_counts [[buffer(0)]],
    device uint* block_offsets [[buffer(1)]],
    device uint* digit_bases [[buffer(2)]],
    constant uint& block_count [[buffer(3)]],
    uint digit [[thread_position_in_threadgroup]]) {
    if (digit < O23_RADIX) {
        uint running = 0;
        for (uint block = 0; block < block_count; ++block) {
            const uint slot = digit * block_count + block;
            block_offsets[slot] = running;
            running += block_counts[slot];
        }
        digit_bases[digit] = running;
    }
    threadgroup_barrier(mem_flags::mem_device);

    if (digit == 0) {
        uint running = 0;
        for (uint current = 0; current < O23_RADIX; ++current) {
            const uint count = digit_bases[current];
            digit_bases[current] = running;
            running += count;
        }
    }
}

O23_SCATTER(0, 0)
O23_SCATTER(1, 4)
O23_SCATTER(2, 8)
O23_SCATTER(3, 12)
O23_SCATTER(4, 16)
O23_SCATTER(5, 20)
O23_SCATTER(6, 24)
O23_SCATTER(7, 28)
O23_SCATTER(8, 32)
O23_SCATTER(9, 36)
O23_SCATTER(10, 40)
O23_SCATTER(11, 44)
O23_SCATTER(12, 48)
O23_SCATTER(13, 52)
O23_SCATTER(14, 56)
O23_SCATTER(15, 60)

kernel void o23_simd_probe(
    device const uint* digits [[buffer(0)]],
    device uint* ranks [[buffer(1)]],
    device uint* geometry [[buffer(2)]],
    uint tid [[thread_position_in_threadgroup]],
    uint simd_lane [[thread_index_in_simdgroup]],
    uint simd_group [[simdgroup_index_in_threadgroup]]) {
    threadgroup uint simd_counts[8 * 16];
    const uint digit = digits[tid];
    uint within_simd = 0;
    for (uint current = 0; current < O23_RADIX; ++current) {
        const uint present = uint(digit == current);
        const uint prefix = simd_prefix_exclusive_sum(present);
        const uint total = simd_sum(present);
        if (simd_lane == current) {
            simd_counts[simd_group * O23_RADIX + current] = total;
        }
        if (digit == current) {
            within_simd = prefix;
        }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    uint rank = within_simd;
    for (uint group = 0; group < simd_group; ++group) {
        rank += simd_counts[group * O23_RADIX + digit];
    }
    ranks[tid] = rank;
    geometry[tid * 2] = simd_lane;
    geometry[tid * 2 + 1] = simd_group;
}
