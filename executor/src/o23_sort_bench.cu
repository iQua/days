#include <stdint.h>

namespace {

constexpr unsigned kBlockThreads = 256;
constexpr unsigned kSimdWidth = 32;
constexpr unsigned kSimdGroups = kBlockThreads / kSimdWidth;
constexpr unsigned kItemsPerThread = 4;
constexpr unsigned kTileKeys = kBlockThreads * kItemsPerThread;
constexpr unsigned kRadix = 16;

__device__ __forceinline__ void count_tiles(
    const uint64_t* keys,
    uint32_t* ranks,
    uint32_t* block_counts,
    uint32_t block_count,
    unsigned shift) {
    __shared__ uint32_t simd_counts[kSimdGroups * kRadix];
    __shared__ uint32_t seen[kRadix];
    const unsigned lane = threadIdx.x & (kSimdWidth - 1);
    const unsigned simd_group = threadIdx.x / kSimdWidth;
    const unsigned tile_base = blockIdx.x * kTileKeys;
    if (threadIdx.x < kRadix) {
        seen[threadIdx.x] = 0;
    }
    __syncthreads();

    for (unsigned item = 0; item < kItemsPerThread; ++item) {
        const unsigned index = tile_base + item * kBlockThreads + threadIdx.x;
        const unsigned digit = static_cast<unsigned>((keys[index] >> shift) & 0xfu);
        uint32_t within_simd = 0;
        for (unsigned current = 0; current < kRadix; ++current) {
            const unsigned mask = __ballot_sync(0xffff'ffffu, digit == current);
            if (lane == current) {
                simd_counts[simd_group * kRadix + current] = __popc(mask);
            }
            if (digit == current) {
                const unsigned lower_lanes = lane == 0 ? 0 : ((1u << lane) - 1u);
                within_simd = __popc(mask & lower_lanes);
            }
        }
        __syncthreads();

        uint32_t local_rank = seen[digit] + within_simd;
        for (unsigned group = 0; group < simd_group; ++group) {
            local_rank += simd_counts[group * kRadix + digit];
        }
        ranks[index] = local_rank;
        __syncthreads();

        if (threadIdx.x < kRadix) {
            uint32_t item_count = 0;
            for (unsigned group = 0; group < kSimdGroups; ++group) {
                item_count += simd_counts[group * kRadix + threadIdx.x];
            }
            seen[threadIdx.x] += item_count;
        }
        __syncthreads();
    }
    if (threadIdx.x < kRadix) {
        block_counts[threadIdx.x * block_count + blockIdx.x] = seen[threadIdx.x];
    }
}

__device__ __forceinline__ void scatter_tiles(
    const uint64_t* input,
    uint64_t* output,
    const uint32_t* ranks,
    const uint32_t* block_offsets,
    const uint32_t* digit_bases,
    uint32_t block_count,
    unsigned shift) {
    const unsigned tile_base = blockIdx.x * kTileKeys;
    for (unsigned item = 0; item < kItemsPerThread; ++item) {
        const unsigned index = tile_base + item * kBlockThreads + threadIdx.x;
        const uint64_t key = input[index];
        const unsigned digit = static_cast<unsigned>((key >> shift) & 0xfu);
        const uint32_t destination = digit_bases[digit]
            + block_offsets[digit * block_count + blockIdx.x]
            + ranks[index];
        output[destination] = key;
    }
}

}  // namespace

#define O23_COUNT(PASS, SHIFT)                                            \
    extern "C" __global__ void o23_count_##PASS(                         \
        const uint64_t* keys,                                              \
        uint32_t* ranks,                                                   \
        uint32_t* block_counts,                                            \
        uint32_t blocks) {                                                 \
        count_tiles(keys, ranks, block_counts, blocks, SHIFT);             \
    }

#define O23_SCATTER(PASS, SHIFT)                                                      \
    extern "C" __global__ void o23_scatter_##PASS(                                  \
        const uint64_t* input,                                                        \
        uint64_t* output,                                                             \
        const uint32_t* ranks,                                                        \
        const uint32_t* block_offsets,                                                \
        const uint32_t* digit_bases,                                                  \
        uint32_t block_count) {                                                       \
        scatter_tiles(                                                                \
            input, output, ranks, block_offsets, digit_bases, block_count, SHIFT);    \
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

extern "C" __global__ void o23_prefix(
    const uint32_t* block_counts,
    uint32_t* block_offsets,
    uint32_t* digit_bases,
    uint32_t block_count) {
    const unsigned digit = threadIdx.x;
    if (digit < kRadix) {
        uint32_t running = 0;
        for (uint32_t block = 0; block < block_count; ++block) {
            const uint32_t slot = digit * block_count + block;
            block_offsets[slot] = running;
            running += block_counts[slot];
        }
        digit_bases[digit] = running;
    }
    __syncthreads();

    if (digit == 0) {
        uint32_t running = 0;
        for (unsigned current = 0; current < kRadix; ++current) {
            const uint32_t count = digit_bases[current];
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
