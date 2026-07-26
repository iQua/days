use std::fs;
use std::path::PathBuf;

use xtask::schema::parse_budget_manifest;

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

    assert_eq!(budget.corpus.len(), 5);
    assert_eq!(budget.method.warmups, 3);
    assert_eq!(budget.method.repetitions, 15);
    assert!(
        budget
            .thresholds
            .iter()
            .any(|threshold| threshold.name == "geometric_mean_no_regression")
    );
    assert!(
        budget
            .thresholds
            .iter()
            .any(|threshold| threshold.name == "per_workload_regression")
    );
}
