//! P16 H3 (aicb): the shared SHA-256 (ruling A6) against FIPS 180-4's vectors and the committed
//! AICB fixtures' recorded hashes (`tests/fixtures/aicb/README.md`).

use std::path::PathBuf;

use days::utils::sha256::sha256_hex;

#[test]
fn sha256_matches_the_standard_vectors() {
    for (message, digest) in [
        (&b""[..], "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        (&b"abc"[..], "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        (
            &b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"[..],
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        ),
        (
            &b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"[..],
            "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1",
        ),
    ] {
        assert_eq!(sha256_hex(message), digest);
    }
    // One million 'a' (FIPS 180-2 appendix B.3).
    assert_eq!(
        sha256_hex(&vec![b'a'; 1_000_000]),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
    // Every padding boundary around one block.
    for length in 50..70 {
        assert_eq!(sha256_hex(&vec![0x5a; length]).len(), 64);
    }
}

#[test]
fn sha256_reproduces_the_fixture_hashes() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb");
    for (name, digest) in [
        (
            "b4-gpt13b-w128-tp8-pp2-gbs8.txt",
            "8268ee8380f9105452428a283713a1e3451fe054635da212aaf9131c09c8a68f",
        ),
        (
            "smoke-moe-w128-tp2-ep32.txt",
            "94cc56eacb18cc7bca660e551859bc21d65fdeaaeec647f8578bc5d72f7ff1b7",
        ),
        (
            "SimAI.conf",
            "1fe56bee9c2a0e0f27fdbe816c2c51c9254b5d5a6c5e05adaac347a758e77fcf",
        ),
    ] {
        assert_eq!(
            sha256_hex(&std::fs::read(dir.join(name)).unwrap()),
            digest,
            "{name}"
        );
    }
}
