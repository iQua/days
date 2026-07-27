//! SHA-256 helpers for versioned evidence.

use std::fs;
use std::io;
use std::path::Path;

use sha2::{Digest, Sha256};

const SHA256_PREFIX: &str = "sha256:";
const SHA256_HEX_LENGTH: usize = 64;

/// Returns the canonical `sha256:<lowercase hex>` digest of `bytes`.
pub fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(SHA256_PREFIX.len() + SHA256_HEX_LENGTH);
    encoded.push_str(SHA256_PREFIX);
    for byte in digest {
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
    }
    encoded
}

/// Reads `path` and returns its canonical SHA-256 digest.
pub fn sha256_file(path: &Path) -> io::Result<String> {
    fs::read(path).map(|bytes| sha256_bytes(&bytes))
}

/// Returns whether `value` is a canonical `sha256:<64 lowercase hex>` digest.
pub fn is_sha256(value: &str) -> bool {
    value.len() == SHA256_PREFIX.len() + SHA256_HEX_LENGTH
        && value.starts_with(SHA256_PREFIX)
        && value[SHA256_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        10..=15 => char::from(b'a' + nibble - 10),
        _ => '0',
    }
}

#[cfg(test)]
mod tests {
    use super::{is_sha256, sha256_bytes};

    #[test]
    fn hashes_bytes_in_canonical_form() {
        assert_eq!(
            sha256_bytes(b"days"),
            "sha256:ab51004e9d71a485f160f655fb9e72bcdef8f5ca4178b26938b49471456fd11c"
        );
    }

    #[test]
    fn validates_only_canonical_hashes() {
        assert!(is_sha256(
            "sha256:79d0e40a1b3c85f72e2af31dd8a12b5929cb5f7ffdcc499964c00b8e84ca3b14"
        ));
        assert!(!is_sha256(
            "sha256:79D0E40A1B3C85F72E2AF31DD8A12B5929CB5F7FFDCC499964C00B8E84CA3B14"
        ));
        assert!(!is_sha256("sha512:abcd"));
    }
}
