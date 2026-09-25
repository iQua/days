//! Exact integer nanosecond arithmetic for constant-rate links.

use std::error::Error;
use std::fmt;

const BITS_PER_BYTE: u128 = 8;
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Failure returned by exact link-time arithmetic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeError {
    /// A constant-rate link must have a nonzero bit rate.
    ZeroRate,
    /// The exact serialization interval does not fit in the public `u64` time domain.
    SerializationOverflow,
    /// Adding serialization and propagation to the start time exceeds `u64`.
    ArrivalOverflow,
}

impl fmt::Display for TimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRate => formatter.write_str("link rate must be greater than zero"),
            Self::SerializationOverflow => {
                formatter.write_str("serialization time exceeds the u64 nanosecond domain")
            }
            Self::ArrivalOverflow => {
                formatter.write_str("link arrival time exceeds the u64 nanosecond domain")
            }
        }
    }
}

impl Error for TimeError {}

/// Computes `ceil(8 * bytes * 10^9 / rate_bps)` exactly in nanoseconds.
///
/// Legacy Nexosim rounds a cumulative floating-point absolute deadline. The executor instead
/// rounds each exact integer service interval up and then adds it to the start time. This
/// intentional semantic difference is the FIFO deadline-rounding finding from P02.
pub fn serialization_time_ns(bytes: u64, rate_bps: u64) -> Result<u64, TimeError> {
    if rate_bps == 0 {
        return Err(TimeError::ZeroRate);
    }

    let numerator = u128::from(bytes) * BITS_PER_BYTE * NANOS_PER_SECOND;
    let divisor = u128::from(rate_bps);
    let quotient = numerator / divisor;
    let rounded = quotient + u128::from(!numerator.is_multiple_of(divisor));

    u64::try_from(rounded).map_err(|_| TimeError::SerializationOverflow)
}

/// Computes the exact arrival time for a non-preemptive constant-rate link.
pub fn link_arrival_time_ns(
    start_time_ns: u64,
    bytes: u64,
    rate_bps: u64,
    propagation_ns: u64,
) -> Result<u64, TimeError> {
    let serialization_ns = serialization_time_ns(bytes, rate_bps)?;

    start_time_ns
        .checked_add(serialization_ns)
        .and_then(|time_ns| time_ns.checked_add(propagation_ns))
        .ok_or(TimeError::ArrivalOverflow)
}
