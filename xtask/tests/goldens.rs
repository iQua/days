use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use xtask::diagnostics::REGISTRY;

#[derive(Debug, Deserialize)]
struct DiagnosticGolden {
    schema_version: u32,
    diagnostics: Vec<DiagnosticEntry>,
}

#[derive(Debug, Deserialize)]
struct DiagnosticEntry {
    code: String,
    slug: String,
    severity: String,
    summary: String,
}

#[derive(Debug, Deserialize)]
struct MutationGolden {
    schema_version: u32,
    mutations: Vec<MutationEntry>,
}

#[derive(Debug, Deserialize)]
struct MutationEntry {
    fault_class: String,
    test_function: String,
    expected_code: String,
}

const MUTATION_COVERAGE_EXEMPTIONS: &[(&str, &str)] = &[(
    "DAYS-AUDIT-0025",
    "internal fail-closed guard covered by audit::tests::unknown_code_fails_closed; no repository mutation can request an unknown compiled diagnostic code",
)];

fn days_gpu_root() -> Option<PathBuf> {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has a repository parent")
        .to_path_buf();
    let root = std::env::var_os("DAYS_GPU_ROOT")
        .map(PathBuf::from)
        .or_else(|| repository.parent().map(|parent| parent.join("days-gpu")))?;
    root.is_dir().then_some(root)
}

#[test]
fn diagnostics_golden_matches_compiled_registry() {
    let Some(days_gpu) = days_gpu_root() else {
        eprintln!("skipping diagnostic golden check: days-gpu is unavailable");
        return;
    };
    let input = fs::read_to_string(days_gpu.join("evidence/P01/diagnostics.toml"))
        .expect("read diagnostic golden");
    let golden: DiagnosticGolden = toml::from_str(&input).expect("parse diagnostic golden");
    assert_eq!(golden.schema_version, 1);
    assert_eq!(golden.diagnostics.len(), REGISTRY.len());
    for (actual, expected) in golden.diagnostics.iter().zip(REGISTRY) {
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.slug, expected.slug);
        assert_eq!(actual.severity, expected.severity.to_string());
        assert_eq!(actual.summary, expected.summary);
    }
}

#[test]
fn mutation_golden_covers_fault_assertions() {
    let Some(days_gpu) = days_gpu_root() else {
        eprintln!("skipping mutation golden check: days-gpu is unavailable");
        return;
    };
    let input = fs::read_to_string(days_gpu.join("evidence/P01/mutation-corpus.toml"))
        .expect("read mutation golden");
    let golden: MutationGolden = toml::from_str(&input).expect("parse mutation golden");
    assert_eq!(golden.schema_version, 1);

    let source = include_str!("phase_audit_mutations.rs");
    let mut source_tests = BTreeSet::new();
    let blocks: Vec<&str> = source.split("#[test]").skip(1).collect();
    for block in &blocks {
        if !block.contains("\"DAYS-AUDIT-") {
            continue;
        }
        let name = block
            .split_once("fn ")
            .and_then(|(_, tail)| tail.split_once('('))
            .map(|(name, _)| name.trim())
            .expect("test has a function name");
        source_tests.insert(name.to_owned());
    }

    let golden_tests: BTreeSet<_> = golden
        .mutations
        .iter()
        .map(|mutation| mutation.test_function.clone())
        .collect();
    assert_eq!(golden_tests, source_tests);

    for mutation in &golden.mutations {
        assert!(!mutation.fault_class.trim().is_empty());
        let needle = format!("\"{}\"", mutation.expected_code);
        let block = blocks
            .iter()
            .find(|block| block.contains(&format!("fn {}(", mutation.test_function)))
            .expect("golden test function exists in mutation source");
        assert!(
            block.contains(&needle),
            "{} does not assert {}",
            mutation.test_function,
            mutation.expected_code
        );
    }

    let covered_codes: BTreeSet<_> = golden
        .mutations
        .iter()
        .map(|mutation| mutation.expected_code.as_str())
        .collect();
    let exempt_codes: BTreeSet<_> = MUTATION_COVERAGE_EXEMPTIONS
        .iter()
        .map(|(code, reason)| {
            assert!(!reason.trim().is_empty(), "{code} exemption needs a reason");
            assert!(
                !covered_codes.contains(code),
                "{code} is both mutation-covered and exempt"
            );
            *code
        })
        .collect();
    let registry_codes: BTreeSet<_> = REGISTRY.iter().map(|entry| entry.code).collect();
    let accounted_codes: BTreeSet<_> = covered_codes.union(&exempt_codes).copied().collect();
    assert_eq!(accounted_codes, registry_codes);
}
