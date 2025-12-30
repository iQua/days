//! Time quantization helpers for the simulation clock.

use std::sync::atomic::{AtomicU64, Ordering};

const NS_PER_SEC: f64 = 1_000_000_000.0;
const RECIP_NS_PER_SEC: f64 = 1.0 / NS_PER_SEC;

static TIME_QUANTUM_NS: AtomicU64 = AtomicU64::new(0);

/// Configures the global time-quantization quantum. A value of 0 disables quantization.
pub fn set_time_quantum_ns(quantum: Option<u64>) {
    TIME_QUANTUM_NS.store(quantum.unwrap_or(0), Ordering::Relaxed);
}

/// Quantizes an absolute timestamp in seconds to the configured quantum.
pub fn quantize_time(time_s: f64) -> f64 {
    let quantum = TIME_QUANTUM_NS.load(Ordering::Relaxed);
    if quantum == 0 {
        return time_s.max(0.0);
    }

    let ns = secs_to_ns(time_s);
    if ns == u128::MAX {
        return f64::MAX;
    }

    let quantum = quantum as u128;
    let rem = ns % quantum;
    if rem == 0 {
        ns_to_secs(ns)
    } else {
        ns_to_secs(ns.saturating_add(quantum - rem))
    }
}

/// Quantizes the sum of the provided time base and delta in seconds.
pub fn quantize_after(base_time_s: f64, delta_s: f64) -> f64 {
    quantize_time(base_time_s + delta_s)
}

fn secs_to_ns(time_s: f64) -> u128 {
    if time_s <= 0.0 {
        return 0;
    }
    // Cap to the maximum value representable as `u128`.
    let scaled = time_s * NS_PER_SEC;
    if scaled >= (u128::MAX as f64) {
        return u128::MAX;
    }
    scaled.round() as u128
}

fn ns_to_secs(ns: u128) -> f64 {
    (ns as f64) * RECIP_NS_PER_SEC
}
