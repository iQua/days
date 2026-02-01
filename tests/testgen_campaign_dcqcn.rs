use std::fs;
use std::path::PathBuf;

use tempfile::tempdir;

use days::utils::testgen::{CampaignArgs, Mutation, TestGenOptions, campaign, seed_index};

#[test]
fn test_campaign_dcqcn_targeted_mutations_in_dry_run() {
    let tmp = tempdir().expect("tempdir");
    let corpus_root = tmp.path().join("corpus");
    let seeds_src = tmp.path().join("seeds_src");
    fs::create_dir_all(&seeds_src).expect("create seeds src");

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let seed_src = repo_root.join("configs").join("dcqcn_simple.toml");
    fs::copy(&seed_src, seeds_src.join("dcqcn_simple.toml")).expect("copy seed");

    let opts = TestGenOptions {
        corpus_root,
        checker_dir: PathBuf::from("lean/.lake/build/bin"),
        leanguard_run: None,
        allow_nondeterministic: false,
        coverage: true,
        tlc_check: false,
        require_tlc_accept: false,
        tlc_spec_dir: PathBuf::from("tla"),
        tlc_bin: None,
        tlc_jar: None,
        tlc_no_dfs: false,
    };

    seed_index(&opts, &seeds_src).expect("seed-index");

    let summary = campaign(
        &opts,
        CampaignArgs {
            protocol: "dcqcn".to_string(),
            budget: 1,
            rng_seed: Some(1),
            goal: None,
            max_calibration_iters: None,
            seed_filter: None,
            dry_run: true,
            use_trace_signature: false,
        },
    )
    .expect("campaign");

    let planned = summary.planned;
    assert_eq!(planned.len(), 1);

    let has_dcqcn_targeted = planned.iter().any(|case| {
        case.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::TweakDcqcnRateGbps { .. }
                    | Mutation::TweakDcqcnMinRateGbps { .. }
                    | Mutation::TweakDcqcnMaxRateGbps { .. }
                    | Mutation::TweakDcqcnG { .. }
                    | Mutation::TweakDcqcnMiFactor { .. }
                    | Mutation::TweakDcqcnAiRateGbps { .. }
                    | Mutation::TweakDcqcnHaiRateGbps { .. }
                    | Mutation::TweakDcqcnCnpIntervalNs { .. }
                    | Mutation::TweakSwitchEcnThreshold { .. }
            )
        })
    });

    assert!(
        has_dcqcn_targeted,
        "expected DCQCN targeted mutation in dry run"
    );
}
