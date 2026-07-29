#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use days_executor::metal_spike::{
    MetalSpikeBenchmarkConfig, benchmark_metal, run_metal_correctness_suite,
};

#[test]
fn metal_demonstrates_required_data_path_primitives() {
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
}

#[test]
fn metal_batches_dependent_dispatches_without_host_readback() {
    let report = benchmark_metal(MetalSpikeBenchmarkConfig {
        dispatch_counts: vec![1, 2, 4],
        samples: 2,
        warmup_samples: 1,
    })
    .expect("Metal dispatch benchmark must execute");

    assert_eq!(report.batches.len(), 3);
    assert!(
        report
            .batches
            .iter()
            .all(|batch| batch.dependency_chain_verified)
    );
    assert!(
        report
            .resident_rounds
            .iter()
            .all(|rounds| rounds.horizon_chain_verified)
    );
}
