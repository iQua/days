//! O1.4 output gate: geometry may change the write cursor, never the canonical worklist.

#![cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]

#[cfg(feature = "cuda-test-hooks")]
const CUDA_THREADS_PER_BLOCK: [usize; 7] = [1, 3, 32, 96, 128, 512, 1_024];
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const METAL_THREADS_PER_THREADGROUP: [usize; 5] = [32, 96, 128, 512, 1_024];

fn eligibility_cases() -> Vec<Vec<bool>> {
    vec![
        Vec::new(),
        vec![true, false, true, true, false, false, true],
        (0..37)
            .map(|node| node % 3 == 0 || node % 11 == 4)
            .collect(),
        (0..130).map(|node| (node * 17 + 5) % 23 < 9).collect(),
    ]
}

fn assert_canonical_outputs(
    backend: &str,
    geometries: &[usize],
    mut run: impl FnMut(&[bool], usize) -> Result<(u64, Vec<u64>), String>,
) {
    for eligible in eligibility_cases() {
        let expected = eligible
            .iter()
            .enumerate()
            .filter_map(|(node, present)| present.then_some(node as u64))
            .collect::<Vec<_>>();
        for &threads in geometries {
            let (active, worklist) = run(&eligible, threads).unwrap_or_else(|error| {
                panic!(
                    "{backend} compaction failed for N={} threads={threads}: {error}",
                    eligible.len()
                )
            });
            assert_eq!(
                active,
                expected.len() as u64,
                "{backend} C_ACTIVE differs for N={} threads={threads}",
                eligible.len()
            );
            assert_eq!(
                worklist,
                expected,
                "{backend} worklist must be ascending for N={} threads={threads}",
                eligible.len()
            );
        }
    }
}

#[cfg(feature = "cuda-test-hooks")]
#[test]
fn cuda_worklist_and_active_count_are_geometry_independent() {
    let executor = days_executor::CudaExecutor::new().expect("CUDA executor must initialize");
    assert_canonical_outputs("CUDA", &CUDA_THREADS_PER_BLOCK, |eligible, threads| {
        executor
            .worklist_compaction_for_testing(eligible, threads)
            .map_err(|error| error.to_string())
    });
}

#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[test]
fn metal_worklist_and_active_count_are_geometry_independent() {
    let executor = days_executor::MetalExecutor::new().expect("Metal executor must initialize");
    assert_canonical_outputs(
        "Metal",
        &METAL_THREADS_PER_THREADGROUP,
        |eligible, threads| {
            executor
                .worklist_compaction_for_testing(eligible, threads)
                .map_err(|error| error.to_string())
        },
    );
}
