//! P14 Lane B fixture gates for the DCQCN controller/CNP and PFC device ports.
//!
//! The fixtures in `configs/p14/` replace the legacy-simulator configs (`configs/dcqcn_*.toml`,
//! `configs/ci/leanguard_{dcqcn,pfc}.toml`, `configs/pfc.toml`), which the current compiler does not
//! lower (orchestrator ruling, 2026-09-27; `days-gpu/evidence/P14/lane-b/report.md`). Each one is
//! pinned three ways:
//!
//! 1. **Mechanism contract**: the counts that make the fixture worth running on a device (CNPs
//!    sourced, PFC control frames delivered, DCQCN transitions, PFC state present) are asserted, so
//!    an edit that stops a fixture exercising its mechanism fails here.
//! 2. **Cross-backend identity**: Scalar and CPU at two worker counts return byte-identical
//!    `RunResult`s. Device identity is asserted by the device conformance tests.
//! 3. **Frozen anchor**: the Scalar `RunResult` fingerprint recorded when the fixture was authored,
//!    the same FNV-1a64 over the pretty `Debug` rendering that `days` prints as `result_fnv1a64`.

use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, CpuConfig, MechanismTransitionRecord, ObservationMode, PacketKind, RunResult,
    SimulationImage, run_cpu_with_observations, run_scalar_with_observations, validate,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: u64,
    fnv1a64: u64,
}

struct FingerprintWriter(Fingerprint);

impl fmt::Write for FingerprintWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.bytes = self
            .0
            .bytes
            .checked_add(value.len() as u64)
            .ok_or(fmt::Error)?;
        self.0.fnv1a64 = value.bytes().fold(self.0.fnv1a64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
        });
        Ok(())
    }
}

fn fingerprint(value: &impl Debug) -> Fingerprint {
    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    write!(&mut writer, "{value:#?}").expect("debug serialization length must fit in u64");
    writer.0
}

fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p14")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

/// What one fixture exercises, counted from a Scalar full-observation run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Coverage {
    /// Switch queues carrying `PfcQueueState`.
    pfc_queues: usize,
    /// PFC ingress monitors with at least one enabled (nonzero XOFF) priority.
    active_monitors: usize,
    cnp_packets: usize,
    /// Delivered PFC control frames (pause or resume).
    pfc_control_transitions: usize,
    dcqcn_transitions: usize,
}

fn coverage(image: &SimulationImage) -> Coverage {
    let queues = || image.switch_states.iter().flat_map(|state| &state.queues);
    let full = run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar full run must succeed");
    let transitions = &full
        .diagnostics
        .as_ref()
        .expect("scalar full observation carries diagnostics")
        .mechanism_transitions;
    Coverage {
        pfc_queues: queues().filter(|queue| queue.pfc.is_some()).count(),
        active_monitors: queues()
            .filter_map(|queue| queue.pfc.as_ref())
            .flat_map(|pfc| &pfc.ingresses)
            .filter(|ingress| ingress.xoff_threshold_bytes.iter().any(|xoff| *xoff != 0))
            .count(),
        cnp_packets: full
            .observed_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)))
            .count(),
        pfc_control_transitions: transitions
            .iter()
            .filter(|record| matches!(record, MechanismTransitionRecord::PfcControl(_)))
            .count(),
        dcqcn_transitions: transitions
            .iter()
            .filter(|record| matches!(record, MechanismTransitionRecord::Dcqcn(_)))
            .count(),
    }
}

/// Scalar is the reference; CPU at two worker counts must match it byte for byte.
fn scalar_cpu_identity(name: &str, image: &SimulationImage) -> RunResult {
    validate(image, Backend::Scalar).unwrap_or_else(|error| panic!("{name} scalar: {error}"));
    let scalar = run_scalar_with_observations(image, None, ObservationMode::Summary)
        .unwrap_or_else(|error| panic!("{name} scalar run: {error:?}"));
    for workers in [2_usize, 4] {
        validate(image, Backend::Cpu { workers })
            .unwrap_or_else(|error| panic!("{name} cpu {workers}: {error}"));
        let cpu = run_cpu_with_observations(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("{name} cpu {workers} run: {error}"));
        assert_eq!(cpu.result, scalar, "{name}: CPU {workers} diverged");
    }
    scalar
}

struct Fixture {
    name: &'static str,
    coverage: Coverage,
    anchor: Fingerprint,
}

const fn fixture(
    name: &'static str,
    (pfc_queues, active_monitors, cnp_packets, pfc_control_transitions, dcqcn_transitions): (
        usize,
        usize,
        usize,
        usize,
        usize,
    ),
    (bytes, fnv1a64): (u64, u64),
) -> Fixture {
    Fixture {
        name,
        coverage: Coverage {
            pfc_queues,
            active_monitors,
            cnp_packets,
            pfc_control_transitions,
            dcqcn_transitions,
        },
        anchor: Fingerprint { bytes, fnv1a64 },
    }
}

/// Frozen at authoring (tree `e625b9e`, 2026-09-27).
const FIXTURES: [Fixture; 9] = [
    fixture(
        "dcqcn_t26.toml",
        (0, 0, 3, 0, 28),
        (8_368, 0x91fc9013a0ea656e),
    ),
    fixture(
        "dcqcn_t26_pfc.toml",
        (4, 2, 3, 40, 28),
        (16_517, 0xd3b43c756519eab9),
    ),
    fixture(
        "dcqcn_simple_zero_xoff.toml",
        (4, 0, 0, 0, 2_200),
        (16_409, 0xb6596e44f813c462),
    ),
    fixture(
        "dcqcn_1s_zero_xoff.toml",
        (4, 0, 0, 0, 10_200),
        (16_411, 0x3af8cda420ad9cf6),
    ),
    fixture(
        "dcqcn_2s_zero_xoff.toml",
        (4, 0, 0, 0, 20_200),
        (16_411, 0x112ca3066f3d3064),
    ),
    fixture(
        "dcqcn_10s_zero_xoff.toml",
        (4, 0, 0, 0, 100_200),
        (16_413, 0x3bb2f491803b6242),
    ),
    fixture(
        "dcqcn_multi_zero_xoff.toml",
        (10, 0, 195, 0, 4_595),
        (47_407, 0x264fae42036b8aea),
    ),
    fixture(
        "leanguard_dcqcn_zero_xoff.toml",
        (4, 0, 0, 0, 2_200),
        (16_409, 0xb6596e44f813c462),
    ),
    fixture(
        "leanguard_pfc_executable.toml",
        (2, 1, 0, 2, 0),
        (9_269, 0x41f1c22a1b14b73b),
    ),
];

#[test]
fn p14_fixtures_exercise_their_mechanisms_and_match_their_frozen_anchors() {
    for fixture in &FIXTURES {
        let image = lower(fixture.name);
        assert_eq!(
            coverage(&image),
            fixture.coverage,
            "{}: mechanism coverage moved",
            fixture.name
        );
        let result = scalar_cpu_identity(fixture.name, &image);
        let actual = fingerprint(&result);
        assert_eq!(
            actual, fixture.anchor,
            "{}: frozen anchor moved (got bytes={} fnv1a64={:016x})",
            fixture.name, actual.bytes, actual.fnv1a64
        );
    }
}

/// Acceptance 1: every P14 fixture reproduces its frozen Scalar anchor on each built device.
#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
#[test]
fn p14_fixtures_match_their_anchors_on_every_built_device() {
    for fixture in &FIXTURES {
        let image = lower(fixture.name);
        #[cfg(all(feature = "metal", target_vendor = "apple"))]
        {
            let run = days_executor::run_metal_with_observations(
                &image,
                None,
                days_executor::MetalConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{} Metal: {error}", fixture.name));
            assert_eq!(
                fingerprint(&run.result),
                fixture.anchor,
                "{} Metal",
                fixture.name
            );
        }
        #[cfg(feature = "cuda")]
        {
            let run = days_executor::run_cuda_with_observations(
                &image,
                None,
                days_executor::CudaConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{} CUDA: {error}", fixture.name));
            assert_eq!(
                fingerprint(&run.result),
                fixture.anchor,
                "{} CUDA",
                fixture.name
            );
        }
    }
}
