use std::fs;
use std::path::PathBuf;

use xtask::schema::{CorpusRole, parse_budget_manifest};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has a repository parent")
        .to_path_buf()
}

#[test]
fn frozen_retirement_budget_matches_schema_and_corpus() {
    let root = repository_root();
    let path = root.join("docs/days-executor/budgets/retirement-budget.toml");
    let input = fs::read_to_string(path).expect("read retirement budget");
    let budget = parse_budget_manifest(&input, &root).expect("validate retirement budget");

    assert_eq!(budget.schema_version.get(), 4);
    assert_eq!(budget.corpus.len(), 13);
    assert_eq!(budget.method.warmups, 3);
    assert_eq!(budget.method.repetitions, 15);
    assert_eq!(budget.platform.expected_num_cpus, 18);
    assert!(!budget.platform.mt_thread_count_source.is_empty());
    assert!(!budget.method.cargo_flags.is_empty());
    assert!(budget.method.minimum_sample_wall_time_seconds > 0.0);
    assert!(budget.method.effective_simulation_duration_seconds > 0.0);
    assert_eq!(
        budget
            .corpus
            .iter()
            .filter(|entry| entry.role == CorpusRole::Correctness)
            .count(),
        5
    );
    assert_eq!(
        budget
            .corpus
            .iter()
            .filter(|entry| entry.role == CorpusRole::Performance)
            .count(),
        8
    );
    assert!(
        budget
            .corpus
            .iter()
            .all(|entry| !entry.workload.is_empty() && !entry.mode.is_empty())
    );
    assert_eq!(budget.admission.evaluated_at, "P23");
    assert!(
        budget
            .admission
            .thresholds
            .iter()
            .any(|threshold| threshold.name == "geometric_mean_no_regression")
    );
    assert!(
        budget
            .admission
            .thresholds
            .iter()
            .filter(|threshold| threshold.name.starts_with("fattree_"))
            .count()
            == 4
    );
    assert_eq!(budget.method.resolved_defaults.len(), 8);
}
