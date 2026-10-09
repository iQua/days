//! SplitMix64's output function, the one integer mixer every seeded derivation of the executor
//! uses (the seeded all-to-all matrix, the ECN ramp's draw). LeanGuard mirrors it in
//! `lean/LeanGuard/P10c/SplitMix.lean`.

/// SplitMix64's output function: a bijection on `u64` whose output bits each depend on every input
/// bit.
pub const fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
