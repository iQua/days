//! Exact conversions used at the legacy simulator's integer event-clock boundary.

use nexosim::time::MonotonicTime;

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

/// Converts a scenario-level seconds value that must be exactly representable in integer
/// nanoseconds.
pub fn scenario_seconds_ns(seconds: f64, label: &str) -> Result<u64, String> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(format!(
            "{label} must be finite and nonnegative, got {seconds}"
        ));
    }
    if seconds == 0.0 {
        return Ok(0);
    }

    let scaled = seconds * NANOS_PER_SECOND;
    if scaled < 1.0 {
        return Err(format!(
            "{label} is a positive sub-nanosecond value: {seconds} seconds"
        ));
    }
    if scaled > u64::MAX as f64 {
        return Err(format!(
            "{label} exceeds the supported u64 nanosecond range: {seconds} seconds"
        ));
    }
    if scaled.fract() != 0.0 {
        return Err(format!(
            "{label} must resolve to an integer number of nanoseconds, got {seconds} seconds"
        ));
    }
    Ok(scaled as u64)
}

/// Converts a positive f64 controller or sampled delay to the earliest non-early integer tick.
pub fn behavior_delay_ns(seconds: f64, label: &str) -> Result<u64, String> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(format!(
            "{label} must be finite and positive, got {seconds}"
        ));
    }
    let scaled = seconds * NANOS_PER_SECOND;
    if scaled > u64::MAX as f64 {
        return Err(format!(
            "{label} exceeds the supported u64 nanosecond range: {seconds} seconds"
        ));
    }
    Ok(scaled.ceil() as u64)
}

/// Returns the exact integer event-clock offset from the simulator epoch.
pub fn clock_ns(time: MonotonicTime) -> u64 {
    u64::try_from(time.duration_since(MonotonicTime::EPOCH).as_nanos())
        .expect("legacy simulation time exceeds the u64 nanosecond clock range")
}

/// Produces the compatibility/controller/report view of an exact event-clock timestamp.
pub fn seconds_view(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / NANOS_PER_SECOND
}

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
