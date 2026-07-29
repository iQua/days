#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use days_executor::metal_spike::{
    ACTIVE_PORT_LPS, MetalSpikeBenchmarkConfig, REDUCTION_LANES, benchmark_metal,
    matched_workload_profile, run_metal_correctness_suite,
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
        round_counts: vec![1_024],
        rounds_per_encoding: 1_024,
        samples: 3,
        warmup_rounds: 32,
    })
    .expect("direct Metal matched benchmark must execute");

    assert_eq!(report.scales.len(), 1);
    assert_eq!(report.scales[0].rounds, 1_024);
    assert_eq!(report.scales[0].encodings, 1);
    assert_eq!(report.scales[0].dispatches_per_round, 2);
    assert_eq!(report.scales[0].host_encode_submit_ns.len(), 3);
    assert_eq!(report.scales[0].device_ns.len(), 3);
    assert_eq!(report.scales[0].gpu_wall_ns.len(), 3);
    assert_eq!(report.scales[0].matched_cpu_ns.len(), 3);
    assert!(report.scales[0].matched_checksums);
    assert!(report.scales[0].no_host_sync_between_rounds);
}
