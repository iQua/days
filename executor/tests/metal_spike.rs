#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use days_executor::metal_spike::{
    ACTIVE_PORT_LPS, DEFAULT_SWEEP_POINTS, MetalSpikeBenchmarkConfig, REDUCTION_LANES, SweepPoint,
    benchmark_metal, matched_workload_profile, run_metal_correctness_suite,
    scaled_workload_profile, sweep_geometry,
};

#[test]
fn matched_workload_carries_the_measured_k32_round_profile() {
    let profile = matched_workload_profile();

    assert_eq!(profile.active_lps, ACTIVE_PORT_LPS);
    assert_eq!(profile.reduction_lanes, REDUCTION_LANES);
    assert_eq!(profile.transitions_per_round, 1_300);
    assert_eq!(profile.fel_pops_per_round, 1_067);
    assert_eq!(profile.local_child_pushes_per_round, 668);
    assert_eq!(profile.same_time_continuations_per_round, 233);
    assert_eq!(profile.occupancy_checks_per_round, 399);
    assert_eq!(profile.outbox_writes_per_round, 399);
    assert_eq!(profile.maximum_transitions_per_lp, 11);
    assert_eq!(profile.observed_tail_maximum_transitions_per_lp, 21);
    assert_eq!(profile.fused_event_packet_bytes, 88);
    assert_eq!(profile.measured_mean_parallel_efficiency_ppm, 208_296);
    assert_eq!(profile.modeled_parallel_efficiency_ppm, 198_625);
}

#[test]
fn sweep_repeats_the_exact_595_lp_profile_and_spans_the_required_range() {
    let widths = DEFAULT_SWEEP_POINTS.map(|point| point.active_lps);

    assert_eq!(
        widths,
        [595, 1_785, 4_760, 20_230, 49_980, 199_920, 499_800]
    );
    assert!(
        DEFAULT_SWEEP_POINTS
            .iter()
            .all(|point| point.active_lps.is_multiple_of(ACTIVE_PORT_LPS))
    );

    let scaled = scaled_workload_profile(4_760).expect("4,760 is eight retained profiles");
    assert_eq!(scaled.active_lps, 4_760);
    assert_eq!(scaled.transitions_per_round, 10_400);
    assert_eq!(scaled.fel_pops_per_round, 8_536);
    assert_eq!(scaled.local_child_pushes_per_round, 5_344);
    assert_eq!(scaled.same_time_continuations_per_round, 1_864);
    assert_eq!(scaled.occupancy_checks_per_round, 3_192);
    assert_eq!(scaled.outbox_writes_per_round, 3_192);
}

#[test]
fn sweep_geometry_reports_the_hierarchical_dispatch_change() {
    let small = sweep_geometry(595).expect("base width is valid");
    assert_eq!(small.padded_lanes, 1_024);
    assert_eq!(small.body_threadgroups, 1);
    assert_eq!(small.reduction_dispatches_per_round, 1);
    assert_eq!(small.dispatches_per_round, 2);
    assert_eq!(small.modeled_threadgroup_core_coverage_ppm(40), 25_000);
    assert_eq!(small.modeled_useful_lane_coverage_ppm(40), 14_526);

    let large = sweep_geometry(499_800).expect("largest required width is valid");
    assert_eq!(large.padded_lanes, 500_736);
    assert_eq!(large.body_threadgroups, 489);
    assert_eq!(large.reduction_dispatches_per_round, 2);
    assert_eq!(large.dispatches_per_round, 3);
    assert_eq!(large.modeled_threadgroup_core_coverage_ppm(40), 1_000_000);
    assert_eq!(large.modeled_useful_lane_coverage_ppm(40), 1_000_000);
}

#[test]
fn direct_metal_revalidates_substrate_affected_primitives() {
    let report = run_metal_correctness_suite().expect("Metal spike must execute on the device");

    assert!(report.fixed_width_u64);
    assert!(report.event_key_total_order);
    assert!(report.exclusive_lp_ownership);
    assert!(report.role_worklists_from_one_image);
    assert!(report.bounded_fel);
    assert!(report.bounded_outbox);
    assert!(report.deterministic_compaction);
    assert!(report.device_horizon_between_dispatches);
    assert!(!report.explicit_inter_dispatch_barrier);
    assert!(report.explicit_device_errors);
    assert!(report.packet_in_event_fusion);
    assert!(report.serial_encoder_dependency_verified);
    assert!(report.single_dispatch_1024_lane_reduction);
    assert!(report.semantic_same_time_continuation_verified);
    assert!(report.continuation_slot_association_verified);
    assert!(report.matched_cpu_gpu_state);
}

#[test]
fn direct_metal_encodes_thousands_of_matched_rounds_without_host_readback() {
    let report = benchmark_metal(MetalSpikeBenchmarkConfig {
        sweep_points: vec![SweepPoint {
            active_lps: 595,
            rounds: 1_024,
            warmup_rounds: 32,
        }],
        rounds_per_encoding: 1_024,
        samples: 3,
        cpu_workers: 4,
    })
    .expect("direct Metal matched benchmark must execute");

    assert_eq!(report.scales.len(), 1);
    assert_eq!(report.cpu_workers, 4);
    assert_eq!(report.scales[0].active_lps, 595);
    assert_eq!(report.scales[0].rounds, 1_024);
    assert_eq!(report.scales[0].encodings, 1);
    assert_eq!(report.scales[0].dispatches_per_round, 2);
    assert_eq!(report.scales[0].host_encode_submit_ns.len(), 3);
    assert_eq!(report.scales[0].device_ns.len(), 3);
    assert_eq!(report.scales[0].gpu_wall_ns.len(), 3);
    assert_eq!(report.scales[0].matched_cpu_ns.len(), 3);
    assert_eq!(report.scales[0].cpu_checksums.len(), 3);
    assert_eq!(report.scales[0].gpu_checksums.len(), 3);
    assert!(report.scales[0].matched_checksums);
    assert!(report.scales[0].no_host_sync_between_rounds);
}

#[test]
fn two_level_reduction_matches_the_four_worker_cpu() {
    let report = benchmark_metal(MetalSpikeBenchmarkConfig {
        sweep_points: vec![SweepPoint {
            active_lps: 1_785,
            rounds: 1_025,
            warmup_rounds: 4,
        }],
        rounds_per_encoding: 1_024,
        samples: 3,
        cpu_workers: 4,
    })
    .expect("hierarchical Metal reduction must match W4 CPU");

    let measurement = &report.scales[0];
    assert_eq!(measurement.encodings, 2);
    assert_eq!(measurement.body_threadgroups, 2);
    assert_eq!(measurement.reduction_dispatches_per_round, 2);
    assert_eq!(measurement.dispatches_per_round, 3);
    assert!(measurement.matched_checksums);
}
