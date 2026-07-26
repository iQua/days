use days::utils::time::{quantize_after, set_time_quantum_ns};
use serde::Deserialize;

const NS_PER_SECOND: f64 = 1_000_000_000.0;
const PS_PER_SECOND: f64 = 1_000_000_000_000.0;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Characterization {
    schema_version: u32,
    policy: String,
    aligned_exact_domain: AlignedExactDomain,
    case: Vec<Case>,
    repeated_fractional_ns: Vec<RepeatedFractionalNs>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlignedExactDomain {
    comparison_boundary: String,
    quantum_ns: u64,
    packet_size_bytes: u64,
    rate_bits_per_second: u64,
    raw_delta_ns: u64,
    min_base_ns: u64,
    max_base_ns_inclusive: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    classification: String,
    quantum_ns: u64,
    packet_size_bytes: u64,
    rate_bits_per_second: u64,
    base_ns_numerator: u64,
    base_ns_denominator: u64,
    delta_ns_numerator: u64,
    delta_ns_denominator: u64,
    expected_output_ns: Option<u64>,
    expected_output_ps: Option<u64>,
    independent_quantized_duration_sum_ns: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepeatedFractionalNs {
    id: String,
    classification: String,
    quantum_ns: u64,
    packet_size_bytes: u64,
    rate_bits_per_second: u64,
    raw_delta_ns_numerator: u64,
    raw_delta_ns_denominator: u64,
    independent_rounded_duration_ns: u64,
    sample: Vec<RepeatedSample>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepeatedSample {
    services: u64,
    absolute_deadline_ps: u64,
    independent_duration_sum_ps: u64,
    divergence_ps: u64,
}

struct ResetQuantum;

impl Drop for ResetQuantum {
    fn drop(&mut self) {
        set_time_quantum_ns(None);
    }
}

fn seconds_from_ns(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64 / NS_PER_SECOND
}

fn observed_ps(time_s: f64) -> u64 {
    (time_s * PS_PER_SECOND).round() as u64
}

#[test]
fn frozen_quantize_after_characterization_matches_legacy_policy() {
    let _reset_quantum = ResetQuantum;
    let golden: Characterization = toml::from_str(include_str!(
        "../docs/days-executor/evidence/P01/quantize-after-characterization.toml"
    ))
    .expect("parse quantize_after characterization");

    assert_eq!(golden.schema_version, 1);
    assert_eq!(golden.policy, "legacy-f64-absolute-deadline");

    let exact = &golden.aligned_exact_domain;
    assert_eq!(exact.comparison_boundary, "exact-ledger");
    assert_eq!(
        exact.packet_size_bytes * 8 * NS_PER_SECOND as u64 / exact.rate_bits_per_second,
        exact.raw_delta_ns
    );
    set_time_quantum_ns(Some(exact.quantum_ns));
    let delta_s = exact.raw_delta_ns as f64 / NS_PER_SECOND;
    for base_ns in exact.min_base_ns..=exact.max_base_ns_inclusive {
        let observed = quantize_after(base_ns as f64 / NS_PER_SECOND, delta_s);
        assert_eq!(
            (observed * NS_PER_SECOND).round() as u64,
            base_ns + exact.raw_delta_ns,
            "aligned exact domain failed at base {base_ns} ns"
        );
    }

    for case in &golden.case {
        assert!(
            matches!(
                case.classification.as_str(),
                "aligned-exact" | "semantic-migration"
            ),
            "invalid classification for {}",
            case.id
        );
        set_time_quantum_ns((case.quantum_ns != 0).then_some(case.quantum_ns));
        assert_eq!(
            case.packet_size_bytes as u128
                * 8
                * NS_PER_SECOND as u128
                * case.delta_ns_denominator as u128,
            case.rate_bits_per_second as u128 * case.delta_ns_numerator as u128,
            "size/rate does not produce the declared delta for {}",
            case.id
        );
        let observed = quantize_after(
            seconds_from_ns(case.base_ns_numerator, case.base_ns_denominator),
            seconds_from_ns(case.delta_ns_numerator, case.delta_ns_denominator),
        );
        match (case.expected_output_ns, case.expected_output_ps) {
            (Some(expected_ns), None) => assert_eq!(
                (observed * NS_PER_SECOND).round() as u64,
                expected_ns,
                "integer-ns output mismatch for {}",
                case.id
            ),
            (None, Some(expected_ps)) => assert_eq!(
                observed_ps(observed),
                expected_ps,
                "fractional-ns output mismatch for {}",
                case.id
            ),
            _ => panic!(
                "{} must declare exactly one expected output representation",
                case.id
            ),
        }
        if let Some(expected_independent_ns) = case.independent_quantized_duration_sum_ns {
            let rounded_duration = quantize_after(
                0.0,
                seconds_from_ns(case.delta_ns_numerator, case.delta_ns_denominator),
            );
            let independent_sum = seconds_from_ns(case.base_ns_numerator, case.base_ns_denominator)
                + rounded_duration;
            assert_eq!(
                (independent_sum * NS_PER_SECOND).round() as u64,
                expected_independent_ns,
                "independently quantized-duration sum mismatch for {}",
                case.id
            );
        }
    }

    for repeated in &golden.repeated_fractional_ns {
        assert_eq!(repeated.classification, "semantic-migration");
        assert_eq!(
            repeated.packet_size_bytes
                * 8
                * NS_PER_SECOND as u64
                * repeated.raw_delta_ns_denominator
                / repeated.rate_bits_per_second,
            repeated.raw_delta_ns_numerator
        );
        set_time_quantum_ns((repeated.quantum_ns != 0).then_some(repeated.quantum_ns));
        let delta_s =
            repeated.packet_size_bytes as f64 * 8.0 / repeated.rate_bits_per_second as f64;
        let mut absolute_deadline = 0.0;
        let mut completed_services = 0;
        for sample in &repeated.sample {
            while completed_services < sample.services {
                absolute_deadline = quantize_after(absolute_deadline, delta_s);
                completed_services += 1;
            }
            let independent_sum_ps =
                sample.services * repeated.independent_rounded_duration_ns * 1_000;
            assert_eq!(
                observed_ps(absolute_deadline),
                sample.absolute_deadline_ps,
                "absolute deadline mismatch for {} at service {}",
                repeated.id,
                sample.services
            );
            assert_eq!(
                independent_sum_ps, sample.independent_duration_sum_ps,
                "independent-duration sum mismatch for {} at service {}",
                repeated.id, sample.services
            );
            assert_eq!(
                independent_sum_ps - observed_ps(absolute_deadline),
                sample.divergence_ps,
                "divergence mismatch for {} at service {}",
                repeated.id,
                sample.services
            );
            if repeated.quantum_ns == 0 && matches!(sample.services, 20 | 100) {
                println!(
                    "5.5ns services={} absolute={}ps independent={}ps divergence={}ps",
                    sample.services,
                    sample.absolute_deadline_ps,
                    sample.independent_duration_sum_ps,
                    sample.divergence_ps
                );
            }
        }
    }
}
