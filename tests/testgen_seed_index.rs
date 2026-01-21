use std::fs;
use std::path::PathBuf;

use tempfile::tempdir;

use days::utils::testgen::{TestGenOptions, seed_index};

#[test]
fn test_seed_index_creates_corpus() {
    let tmp = tempdir().expect("tempdir");
    let corpus_root = tmp.path().join("corpus");
    let seeds_src = tmp.path().join("seeds_src");
    fs::create_dir_all(&seeds_src).expect("create seeds src");

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let seed_src = repo_root.join("configs").join("simple.toml");
    fs::copy(&seed_src, seeds_src.join("simple.toml")).expect("copy seed");

    let opts = TestGenOptions {
        corpus_root: corpus_root.clone(),
        checker_dir: PathBuf::from("lean/.lake/build/bin"),
        leanguard_run: None,
        allow_nondeterministic: false,
    };

    let summary = seed_index(&opts, &seeds_src).expect("seed-index");
    assert!(summary.seeds_added >= 1);
    assert!(corpus_root.join("seeds").exists());
    assert!(
        corpus_root
            .join("metadata")
            .join("seeds_index.json")
            .exists()
    );
}
