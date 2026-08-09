//! Exact conversions used at the legacy simulator's integer event-clock boundary.

/// Returns the integer-nanosecond serialization delay, rounded up so every positive packet
/// advances the event clock.
pub fn serialization_ns(bytes: usize, rate_bps: f64) -> Result<u64, String> {
    if !rate_bps.is_finite() || rate_bps <= 0.0 || rate_bps.fract() != 0.0 {
        return Err(format!(
            "port rate must be a finite positive integer in bits per second, got {rate_bps}"
        ));
    }
    if rate_bps > u64::MAX as f64 {
        return Err(format!(
            "port rate exceeds the supported u64 range in bits per second: {rate_bps}"
        ));
    }

    let numerator = (bytes as u128)
        .checked_mul(8)
        .and_then(|bits| bits.checked_mul(1_000_000_000))
        .ok_or_else(|| format!("packet size is too large to serialize: {bytes} bytes"))?;
    let denominator = rate_bps as u128;
    let quotient = numerator / denominator;
    let rounded_up = quotient + u128::from(numerator % denominator != 0);

    u64::try_from(rounded_up)
        .map_err(|_| "serialization delay exceeds the supported u64 nanosecond range".to_string())
}
