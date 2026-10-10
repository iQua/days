//! P18 diet-host c4: validating once for a device accepts exactly what the two validations did.
//!
//! Before c4 a device run's image passed the Scalar validation (inside `compile`) and then the
//! device's (at the device entry). `ValidatedImage::new(image, device)` runs the Scalar validation
//! and only the device's own refusals. The two agree because every other device-dependent check
//! is weaker than Scalar's: the global-time bound sums a subset of the initial packets' service
//! time on a device, and its lookups are the ones `validate_events` already made. This test
//! checks the agreement on the lowered corpus, for both devices: the same verdict, and the same
//! error when both reject.
//!
//! Run: `cargo test -p days --test p18_validated_image_equivalence` (default matrix).

use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{Backend, ValidatedImage, validate};

/// Every scenario in these directories that lowers.
const CORPUS: &[&str] = &[
    "configs/ci",
    "configs/p14",
    "configs/p15",
    "configs/p16",
    "configs/p17",
];
const AICB: &[&str] = &[
    "tests/fixtures/aicb/b4-simai.toml",
    "tests/fixtures/aicb/b4-megatron.toml",
    "tests/fixtures/aicb/reduced-dense-simai.toml",
    "tests/fixtures/aicb/reduced-dense-megatron.toml",
    "tests/fixtures/aicb/reduced-moe-simai.toml",
    "tests/fixtures/aicb/reduced-moe-imbalanced.toml",
];

fn scenarios() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut paths = CORPUS
        .iter()
        .flat_map(|directory| {
            std::fs::read_dir(root.join(directory))
                .unwrap_or_else(|error| panic!("{directory}: {error}"))
                .map(|entry| entry.expect("directory entry").path())
                .filter(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "toml")
                })
                .collect::<Vec<_>>()
        })
        .chain(AICB.iter().map(|relative| root.join(relative)))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[test]
fn validating_once_for_a_device_matches_scalar_then_device() {
    let mut checked = 0;
    for path in scenarios() {
        let Ok(image) = compile_config(&path) else {
            continue;
        };
        for device in [Backend::Cuda, Backend::Metal] {
            let before = validate(&image, Backend::Scalar).and_then(|()| validate(&image, device));
            let once = ValidatedImage::new(image.clone(), device).map(|_| ());
            assert_eq!(once, before, "{} {device}", path.display());
            checked += 1;
        }
    }
    println!("record=p18_validated_image_equivalence images_times_devices={checked}");
    assert!(checked >= 40, "the corpus lowers: {checked} checks");
}
