//! P18 diet-host c4: validating once for a device accepts exactly what the two validations did.
//!
//! Before c4 a device run's image passed the Scalar validation (inside `compile`) and then the
//! device's (at the device entry). `ValidatedImage::new(image, device)` runs the Scalar validation
//! and only the device's own refusals. The two agree because every other device-dependent check
//! is weaker than Scalar's: the global-time bound sums a subset of the initial packets' service
//! time on a device, and its lookups are the ones `validate_events` already made. This test
//! checks the agreement on the lowered corpus, for both devices: the same verdict, and the same
//! error when both reject. Every corpus image is device-valid, so a second test mutates a lowered
//! image into each class of device-only refusal (a zero-delay channel, a queue over `u32::MAX`
//! packets, an over-wide WFQ weight sum, a non-canonical WFQ checkpoint): Scalar accepts each, and
//! both paths must refuse it with the same error (review M1: without the device refusals in
//! `ValidatedImage::new`, the corpus test alone stayed green).
//!
//! Run: `cargo test -p days --test p18_validated_image_equivalence` (default matrix).

use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{
    Backend, ExactRational, SchedulerKind, SimulationImage, ValidatedImage, WfqSchedulerState,
    validate,
};
use num_bigint::BigUint;

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

/// A lowered image mutated into each device-only refusal class, with the class's name.
fn device_only_invalid_images() -> Vec<(&'static str, SimulationImage)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/ci/executor_smoke.toml");
    let image = compile_config(&path).expect("the CI smoke lowers");
    let mut zero_delay = image.clone();
    zero_delay.channels[0].min_delay_ns = 0;
    let mut wide_queue = image.clone();
    wide_queue.switch_states[0].queues[0].queue_capacity_packets = u64::from(u32::MAX) + 1;
    let mut wide_weights = image.clone();
    wide_weights.switch_states[0].queues[0].scheduler =
        SchedulerKind::weighted_fair_queue(vec![18_446_744_074]);
    let mut noncanonical = image;
    let mut state = WfqSchedulerState::new(vec![1]);
    state.finish_times[0] = ExactRational::new_raw(BigUint::from(2_u8), BigUint::from(2_u8));
    noncanonical.switch_states[0].queues[0].scheduler = SchedulerKind::WeightedFairQueue(state);
    vec![
        ("zero-delay channel", zero_delay),
        ("queue over u32::MAX", wide_queue),
        ("WFQ weight sum", wide_weights),
        ("non-canonical WFQ checkpoint", noncanonical),
    ]
}

#[test]
fn validating_once_for_a_device_keeps_every_device_only_refusal() {
    let mut lost = Vec::new();
    for (class, image) in device_only_invalid_images() {
        validate(&image, Backend::Scalar)
            .unwrap_or_else(|error| panic!("{class}: Scalar accepts it ({error})"));
        for device in [Backend::Cuda, Backend::Metal] {
            let before = validate(&image, Backend::Scalar).and_then(|()| validate(&image, device));
            let once = ValidatedImage::new(image.clone(), device).map(|_| ());
            println!(
                "record=p18_device_only_refusal class={class:?} device={device} once={once:?}"
            );
            assert!(
                before.is_err(),
                "{class} {device}: the device validation refuses it"
            );
            if once != before {
                lost.push(format!("{class} {device}: {once:?} instead of {before:?}"));
            }
        }
    }
    assert!(
        lost.is_empty(),
        "validating once lost device-only refusals:\n{}",
        lost.join("\n")
    );
}
