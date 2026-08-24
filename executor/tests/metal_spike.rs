#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use days_executor::metal_spike::{
    ACTIVE_PORT_LPS, DEFAULT_SWEEP_POINTS, MetalSpikeBenchmarkConfig, REDUCTION_LANES,
    RealReplayBenchmarkConfig, RealReplayLp, RealReplayRound, RealReplayTrace, ReplayStep,
    SweepPoint, benchmark_metal, benchmark_real_replay, matched_workload_profile,
    run_metal_correctness_suite, scaled_workload_profile, sweep_geometry,
};
use days_executor::{EventKind, NodeId};

#[test]
fn real_replay_step_retains_kind_order_children_and_queue_occupancy() {
    let step = ReplayStep::new(EventKind::TxReady, true, 2, 1, Some(100))
        .expect("the v1 fixture fits the packed replay step");

    assert_eq!(step.kind(), EventKind::TxReady);
    assert!(step.is_direct_continuation());
    assert_eq!(step.local_fel_pushes(), 2);
    assert_eq!(step.remote_outbox_writes(), 1);
    assert_eq!(step.queue_occupancy(), Some(100));

    let remote = ReplayStep::new(EventKind::RemoteArrival, false, 0, 0, None)
        .expect("a non-TxReady step has no queue observation");
    assert_eq!(remote.kind(), EventKind::RemoteArrival);
    assert_eq!(remote.queue_occupancy(), None);
}

#[test]
fn skewed_real_trace_replay_matches_persistent_cpu_and_metal() {
    let steps = vec![
        ReplayStep::new(EventKind::RemoteArrival, false, 1, 0, None).unwrap(),
        ReplayStep::new(EventKind::TxComplete, false, 0, 0, None).unwrap(),
        ReplayStep::new(EventKind::TxReady, true, 1, 1, Some(3)).unwrap(),
        ReplayStep::new(EventKind::PacketArrival, false, 1, 0, None).unwrap(),
        ReplayStep::new(EventKind::TxReady, false, 2, 2, Some(1)).unwrap(),
    ];
    let trace = RealReplayTrace {
        source_round_count: 2,
        rounds: vec![
            RealReplayRound {
                source_round: 0,
                frontier_ns: 10,
                exclusive_horizon_ns: 20,
                events_processed: 4,
                active_lp_count: 2,
                maximum_events_per_lp: 3,
                parallel_efficiency: 2.0 / 3.0,
                lp_start: 0,
                lp_count: 2,
            },
            RealReplayRound {
                source_round: 1,
                frontier_ns: 20,
                exclusive_horizon_ns: 30,
                events_processed: 1,
                active_lp_count: 1,
                maximum_events_per_lp: 1,
                parallel_efficiency: 1.0,
                lp_start: 2,
                lp_count: 1,
            },
        ],
        lps: vec![
            RealReplayLp {
                node: NodeId(3),
                pending_events_below_horizon: 2,
                next_time_ns_after_local_drain: 30,
                step_start: 0,
                step_count: 3,
            },
            RealReplayLp {
                node: NodeId(7),
                pending_events_below_horizon: 1,
                next_time_ns_after_local_drain: 32,
                step_start: 3,
                step_count: 1,
            },
            RealReplayLp {
                node: NodeId(3),
                pending_events_below_horizon: 1,
                next_time_ns_after_local_drain: 40,
                step_start: 4,
                step_count: 1,
            },
        ],
        steps,
    };
    trace.validate().unwrap();
    let warmup = trace.selected_rounds(&[0]).unwrap();
    let measured = trace.selected_rounds(&[0, 1]).unwrap();

    let report = benchmark_real_replay(
        &warmup,
        &measured,
        RealReplayBenchmarkConfig {
            samples: 4,
            rounds_per_encoding: 1_024,
            cpu_worker_counts: vec![1, 2],
        },
    )
    .expect("the exact skewed trace should match on CPU and Metal");

    assert_eq!(report.rounds, 2);
    assert_eq!(report.samples.len(), 4);
    assert_eq!(report.cpu_worker_counts, vec![1, 2]);
    assert!(report.matched_checksums);
    assert!(report.no_host_sync_between_rounds);
    assert_eq!(
        report.resident_parent_stream_bytes,
        measured.steps.len() * 88
    );
    assert_eq!(report.local_fel_fused_bytes, report.padded_lanes * 2 * 88);
    assert_eq!(
        report.remote_outbox_fused_bytes,
        report.padded_lanes * 2 * 88
    );
    assert!(report.trace_consistent_horizon_dependency);
    assert!(report.variable_active_lp_guard);
    assert_eq!(report.profile.minimum_active_lps, 1);
    assert_eq!(report.profile.maximum_active_lps, 2);
}

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
