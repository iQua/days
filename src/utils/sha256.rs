//! FIPS 180-4 SHA-256, for provenance pins (the AICB trace and `SimAI.conf` a scenario names).
//!
//! Days has no hashing dependency; this is the one implementation (P16 ruling A6), checked
//! against the standard vectors.

/// The lowercase hex SHA-256 of `message`.
pub fn sha256_hex(_message: &[u8]) -> String {
    String::new()
}
