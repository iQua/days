//! The image fingerprint shared by the lowering gates (`#[path]`-included; not a test target):
//! FNV-1a-64 over the pretty Debug rendering `{image:#?}`, with its byte count.
#![allow(dead_code)]

use std::fmt;

use days_executor::SimulationImage;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageFingerprint {
    pub flows: usize,
    pub debug_bytes: usize,
    pub debug_fnv1a64: u64,
}

/// Streams `{:#?}` through an FNV-1a-64 accumulator without materializing the Debug text.
///
/// The frontier image formats to hundreds of megabytes and the flagship's to tens of gigabytes,
/// so the fingerprint must never be taken over an intermediate `String`.
pub fn fingerprint(image: &SimulationImage) -> ImageFingerprint {
    struct Fnv1a64 {
        hash: u64,
        bytes: usize,
    }

    impl fmt::Write for Fnv1a64 {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            self.bytes += value.len();
            self.hash = value.bytes().fold(self.hash, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
            });
            Ok(())
        }
    }

    let mut accumulator = Fnv1a64 {
        hash: 0xcbf2_9ce4_8422_2325,
        bytes: 0,
    };
    fmt::write(&mut accumulator, format_args!("{image:#?}"))
        .expect("formatting into the image hasher should succeed");
    ImageFingerprint {
        flows: image.flows.len(),
        debug_bytes: accumulator.bytes,
        debug_fnv1a64: accumulator.hash,
    }
}
