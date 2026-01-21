use rand::prelude::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct TestGenOptions {
    pub corpus_root: PathBuf,
    pub checker_dir: PathBuf,
    pub leanguard_run: Option<PathBuf>,
    pub allow_nondeterministic: bool,
}

#[derive(Debug, Serialize)]
pub struct SeedIndexSummary {
    pub seeds_source: String,
    pub corpus_root: String,
    pub seeds_added: usize,
    pub seeds_index_path: String,
}

#[derive(Debug, Serialize)]
pub struct FuzzSummary {
    pub budget: usize,
    pub attempted: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub errors: usize,
    pub corpus_root: String,
}

#[derive(Debug, Serialize)]
pub struct ReplaySummary {
    pub case_dir: String,
    pub accept: bool,
    pub run_summary_path: String,
}

#[derive(Debug, Serialize)]
pub struct MinimizeSummary {
    pub case_dir: String,
    pub iterations: usize,
    pub kept_changes: usize,
    pub accept: bool,
    pub minimized_config_path: String,
}

#[derive(Debug, Serialize)]
struct SeedIndexV1 {
    version: u32,
    seeds: Vec<SeedIndexEntry>,
}

#[derive(Debug, Serialize)]
struct SeedIndexEntry {
    seed_id: String,
    source_path: String,
    seed_path: String,
    required_features: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Mutation {
    SetLogPath {
        value: String,
    },
    SetThreadingSingle,
    SetSeed {
        value: u64,
    },
    TweakDuration {
        from: f64,
        to: f64,
    },
    TweakInitialDelay {
        from: f64,
        to: f64,
    },
    TweakFlowCount {
        from: u32,
        to: u32,
    },
    TweakDistribution {
        field: String,
        dist_type: String,
        from: String,
        to: String,
    },
    TweakSwitchPortRate {
        from: f64,
        to: f64,
    },
    TweakSwitchCapacity {
        from: i64,
        to: i64,
    },
    ShuffleEdges,
}

#[derive(Debug, Serialize)]
struct CaseMetadataV1 {
    version: u32,
    case_id: String,
    created_at_unix_s: u64,
    parent: CaseParent,
    paths: CasePaths,
    mutations: Vec<Mutation>,
    result: CaseResult,
    coverage: CoverageInfo,
}

#[derive(Debug, Serialize)]
struct CaseParent {
    kind: String,
    id: String,
    path: String,
}

#[derive(Debug, Serialize)]
struct CasePaths {
    case_dir: String,
    config: String,
    log_dir: String,
    run_summary: String,
}

#[derive(Debug, Serialize)]
struct CaseResult {
    accept: bool,
    days_status: String,
    days_error: Option<String>,
    trace_discovery_mode: Option<String>,
    traces: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CoverageInfo {
    mode: String,
    observed: Vec<String>,
    novelty: String,
}

#[derive(Debug, Clone)]
struct CorpusPaths {
    root: PathBuf,
    seeds_dir: PathBuf,
    accepted_dir: PathBuf,
    rejected_dir: PathBuf,
    metadata_dir: PathBuf,
    work_dir: PathBuf,
}

impl CorpusPaths {
    fn new(root: &Path) -> Self {
        let root = root.to_path_buf();
        Self {
            seeds_dir: root.join("seeds"),
            accepted_dir: root.join("accepted"),
            rejected_dir: root.join("rejected"),
            metadata_dir: root.join("metadata"),
            work_dir: root.join("_work"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), String> {
        fs::create_dir_all(&self.seeds_dir)
            .map_err(|e| format!("Failed to create seeds dir: {e}"))?;
        fs::create_dir_all(&self.accepted_dir)
            .map_err(|e| format!("Failed to create accepted dir: {e}"))?;
        fs::create_dir_all(&self.rejected_dir)
            .map_err(|e| format!("Failed to create rejected dir: {e}"))?;
        fs::create_dir_all(&self.metadata_dir)
            .map_err(|e| format!("Failed to create metadata dir: {e}"))?;
        fs::create_dir_all(&self.work_dir)
            .map_err(|e| format!("Failed to create work dir: {e}"))?;
        Ok(())
    }
}

pub fn seed_index(opts: &TestGenOptions, seeds_src: &Path) -> Result<SeedIndexSummary, String> {
    let corpus = CorpusPaths::new(&opts.corpus_root);
    corpus.ensure()?;

    let mut seeds = Vec::new();
    collect_toml_files(seeds_src, &mut seeds)?;

    let mut used_ids: HashMap<String, usize> = HashMap::new();
    let mut entries = Vec::new();

    for seed_path in seeds {
        let rel = seed_path.strip_prefix(seeds_src).unwrap_or(&seed_path);
        let mut seed_id = sanitize_id(rel);
        if seed_id.is_empty() {
            seed_id = format!("seed_{}", entries.len());
        }
        if let Some(count) = used_ids.get_mut(&seed_id) {
            *count += 1;
            seed_id = format!("{seed_id}_{}", count);
        } else {
            used_ids.insert(seed_id.clone(), 0);
        }

        let dest_path = corpus.seeds_dir.join(format!("{seed_id}.toml"));
        fs::copy(&seed_path, &dest_path)
            .map_err(|e| format!("Failed to copy seed {}: {e}", seed_path.display()))?;

        let required_features = detect_required_features(&seed_path)?;
        entries.push(SeedIndexEntry {
            seed_id,
            source_path: seed_path.display().to_string(),
            seed_path: dest_path.display().to_string(),
            required_features,
        });
    }

    let index = SeedIndexV1 {
        version: 1,
        seeds: entries,
    };
    let index_path = corpus.metadata_dir.join("seeds_index.json");
    write_json(&index_path, &index)?;

    Ok(SeedIndexSummary {
        seeds_source: seeds_src.display().to_string(),
        corpus_root: corpus.root.display().to_string(),
        seeds_added: index.seeds.len(),
        seeds_index_path: index_path.display().to_string(),
    })
}

pub fn fuzz(
    opts: &TestGenOptions,
    budget: usize,
    rng_seed: Option<u64>,
) -> Result<FuzzSummary, String> {
    let corpus = CorpusPaths::new(&opts.corpus_root);
    corpus.ensure()?;

    let mut seeds = Vec::new();
    collect_toml_files(&corpus.seeds_dir, &mut seeds)?;
    if seeds.is_empty() {
        return Err("No seeds found; run `leanguard-testgen seed-index` first.".to_string());
    }

    let seed = rng_seed.unwrap_or_else(default_rng_seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let leanguard_run = resolve_leanguard_run(opts.leanguard_run.as_deref())?;

    let mut attempted = 0;
    let mut accepted = 0;
    let mut rejected = 0;
    let mut errors = 0;

    for i in 0..budget {
        attempted += 1;
        let seed_path = seeds.choose(&mut rng).unwrap().clone();
        let seed_config = read_toml(&seed_path)?;

        let case_id = format!("{}_{}", unix_nanos(), i);
        let work_dir = corpus.work_dir.join(&case_id);
        fs::create_dir_all(&work_dir).map_err(|e| format!("Failed to create case dir: {e}"))?;

        let log_dir = work_dir.join("logs");
        let mut config = seed_config;
        let mut mutations = apply_base_overrides(&mut config, &log_dir, rng.next_u64(), true);
        mutations.extend(apply_random_mutations(&mut config, &mut rng));

        let config_path = work_dir.join("config.toml");
        write_toml(&config_path, &config)?;

        let run = run_leanguard(
            &leanguard_run,
            &opts.checker_dir,
            &config_path,
            opts.allow_nondeterministic,
        );

        let (accept, run_summary, parsed) = match run {
            Ok(output) => {
                let json = extract_json(&output.stdout).unwrap_or(output.stdout);
                let parsed = parse_run_summary(&json);
                let accept = parsed.as_ref().map(|p| p.accept).unwrap_or(false);
                (accept, json, parsed)
            }
            Err(e) => {
                errors += 1;
                let json_value =
                    serde_json::json!({"version":1,"accept":false,"days":"error","days_error":e});
                let json = serde_json::to_string_pretty(&json_value)
                    .unwrap_or_else(|_| "{\"version\":1,\"accept\":false}".to_string());
                (false, json, Err(e))
            }
        };

        let final_dir = if accept {
            accepted += 1;
            corpus.accepted_dir.join(&case_id)
        } else {
            rejected += 1;
            corpus.rejected_dir.join(&case_id)
        };
        fs::create_dir_all(&final_dir)
            .map_err(|e| format!("Failed to create final case dir: {e}"))?;

        let final_config_path = final_dir.join("config.toml");
        let final_log_dir = final_dir.join("logs");
        let final_run_summary_path = final_dir.join("run_summary.json");

        if log_dir.exists() {
            fs::rename(&log_dir, &final_log_dir)
                .map_err(|e| format!("Failed to move logs: {e}"))?;
        } else {
            fs::create_dir_all(&final_log_dir)
                .map_err(|e| format!("Failed to create log dir: {e}"))?;
        }

        let mut final_config = read_toml(&config_path)?;
        apply_log_path(&mut final_config, &final_log_dir);
        write_toml(&final_config_path, &final_config)?;

        let mut run_summary_value = serde_json::from_str::<serde_json::Value>(&run_summary)
            .unwrap_or_else(|_| serde_json::json!({"version":1,"accept":false}));
        if let Some(obj) = run_summary_value.as_object_mut() {
            obj.insert(
                "config_path".to_string(),
                serde_json::Value::String(final_config_path.display().to_string()),
            );
            obj.insert(
                "log_path".to_string(),
                serde_json::Value::String(final_log_dir.display().to_string()),
            );
        }
        write_json(&final_run_summary_path, &run_summary_value)?;

        let mutations_path = final_dir.join("mutations.json");
        write_json(&mutations_path, &mutations)?;

        let metadata_path = corpus.metadata_dir.join(format!("{case_id}.json"));
        let parent = CaseParent {
            kind: "seed".to_string(),
            id: seed_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string(),
            path: seed_path.display().to_string(),
        };
        let metadata = build_case_metadata(BuildCaseMetadataArgs {
            case_id: &case_id,
            parent,
            case_dir: &final_dir,
            config_path: &final_config_path,
            log_dir: &final_log_dir,
            run_summary_path: &final_run_summary_path,
            mutations,
            parsed: parsed.ok(),
        });
        write_json(&metadata_path, &metadata)?;

        fs::remove_dir_all(&work_dir).ok();
    }

    Ok(FuzzSummary {
        budget,
        attempted,
        accepted,
        rejected,
        errors,
        corpus_root: corpus.root.display().to_string(),
    })
}

pub fn replay(opts: &TestGenOptions, case_dir: &Path) -> Result<ReplaySummary, String> {
    let corpus = CorpusPaths::new(&opts.corpus_root);
    corpus.ensure()?;

    let config_path = case_dir.join("config.toml");
    if !config_path.exists() {
        return Err(format!("Missing config.toml under {}", case_dir.display()));
    }

    let leanguard_run = resolve_leanguard_run(opts.leanguard_run.as_deref())?;
    let log_dir = case_dir.join("logs");

    let mut config = read_toml(&config_path)?;
    apply_log_path(&mut config, &log_dir);
    write_toml(&config_path, &config)?;

    let output = run_leanguard(
        &leanguard_run,
        &opts.checker_dir,
        &config_path,
        opts.allow_nondeterministic,
    )?;

    let json = extract_json(&output.stdout).unwrap_or(output.stdout);
    let parsed = parse_run_summary(&json);
    let accept = parsed.as_ref().map(|p| p.accept).unwrap_or(false);

    let run_summary_path = case_dir.join("run_summary.json");
    let mut run_summary_value =
        serde_json::from_str::<serde_json::Value>(&json).unwrap_or_else(|_| {
            serde_json::json!({"version":1,"accept":false,"days":"error","days_error":"invalid JSON"})
        });
    if let Some(obj) = run_summary_value.as_object_mut() {
        obj.insert(
            "config_path".to_string(),
            serde_json::Value::String(config_path.display().to_string()),
        );
        obj.insert(
            "log_path".to_string(),
            serde_json::Value::String(log_dir.display().to_string()),
        );
    }
    write_json(&run_summary_path, &run_summary_value)?;

    let case_id = case_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();
    let metadata_path = corpus.metadata_dir.join(format!("{case_id}.json"));
    let metadata = build_case_metadata(BuildCaseMetadataArgs {
        case_id: &case_id,
        parent: CaseParent {
            kind: "case".to_string(),
            id: case_id.clone(),
            path: case_dir.display().to_string(),
        },
        case_dir,
        config_path: &config_path,
        log_dir: &log_dir,
        run_summary_path: &run_summary_path,
        mutations: Vec::new(),
        parsed: parsed.ok(),
    });
    write_json(&metadata_path, &metadata)?;

    Ok(ReplaySummary {
        case_dir: case_dir.display().to_string(),
        accept,
        run_summary_path: run_summary_path.display().to_string(),
    })
}

pub fn minimize(
    opts: &TestGenOptions,
    case_dir: &Path,
    max_iters: usize,
) -> Result<MinimizeSummary, String> {
    let config_path = case_dir.join("config.toml");
    if !config_path.exists() {
        return Err(format!("Missing config.toml under {}", case_dir.display()));
    }

    let leanguard_run = resolve_leanguard_run(opts.leanguard_run.as_deref())?;
    let work_dir = case_dir.join("minimize_work");
    fs::create_dir_all(&work_dir)
        .map_err(|e| format!("Failed to create minimize work dir: {e}"))?;

    let baseline_accept = if let Ok(content) = fs::read_to_string(case_dir.join("run_summary.json"))
    {
        parse_run_summary(&content)
            .map(|p| p.accept)
            .unwrap_or(false)
    } else {
        let output = run_leanguard(
            &leanguard_run,
            &opts.checker_dir,
            &config_path,
            opts.allow_nondeterministic,
        )?;
        let json = extract_json(&output.stdout).unwrap_or(output.stdout);
        parse_run_summary(&json).map(|p| p.accept).unwrap_or(false)
    };

    if baseline_accept {
        return Err("Case is already accepted; minimize expects a failing case.".to_string());
    }

    let mut current = read_toml(&config_path)?;
    let mut kept_changes = 0;
    let mut iterations = 0;

    type ShrinkerFn = fn(&toml::Value) -> Option<(toml::Value, Mutation)>;
    let shrinkers: Vec<ShrinkerFn> = vec![
        shrink_duration,
        shrink_flow_count,
        shrink_traffic_size_or_duration,
        shrink_uniform_ranges,
    ];

    for shrinker in shrinkers {
        if iterations >= max_iters {
            break;
        }

        if let Some((mut candidate, _mutation)) = shrinker(&current) {
            iterations += 1;
            let log_dir = work_dir.join(format!("logs_{iterations}"));
            apply_log_path(&mut candidate, &log_dir);

            let candidate_path = work_dir.join("candidate.toml");
            write_toml(&candidate_path, &candidate)?;

            let output = run_leanguard(
                &leanguard_run,
                &opts.checker_dir,
                &candidate_path,
                opts.allow_nondeterministic,
            );
            if let Ok(output) = output {
                let json = extract_json(&output.stdout).unwrap_or(output.stdout);
                if let Ok(parsed) = parse_run_summary(&json) {
                    if !parsed.accept {
                        current = candidate;
                        kept_changes += 1;
                    }
                }
            }
        }
    }

    if kept_changes > 0 {
        let backup_path = case_dir.join("config.orig.toml");
        if !backup_path.exists() {
            fs::copy(&config_path, &backup_path)
                .map_err(|e| format!("Failed to back up config: {e}"))?;
        }
        write_toml(&config_path, &current)?;
    }

    let accept = false;
    Ok(MinimizeSummary {
        case_dir: case_dir.display().to_string(),
        iterations,
        kept_changes,
        accept,
        minimized_config_path: config_path.display().to_string(),
    })
}

#[derive(Debug)]
struct ParsedRunSummary {
    accept: bool,
    days_status: String,
    days_error: Option<String>,
    trace_mode: Option<String>,
    traces: Vec<String>,
}

fn parse_run_summary(json: &str) -> Result<ParsedRunSummary, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("Failed to parse run summary: {e}"))?;
    let accept = value
        .get("accept")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let days_status = value
        .get("days")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let days_error = value
        .get("days_error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let trace_discovery = value.get("trace_discovery");
    let trace_mode = trace_discovery
        .and_then(|v| v.get("mode"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let traces = trace_discovery
        .and_then(|v| v.get("traces"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(ParsedRunSummary {
        accept,
        days_status,
        days_error,
        trace_mode,
        traces,
    })
}

struct BuildCaseMetadataArgs<'a> {
    case_id: &'a str,
    parent: CaseParent,
    case_dir: &'a Path,
    config_path: &'a Path,
    log_dir: &'a Path,
    run_summary_path: &'a Path,
    mutations: Vec<Mutation>,
    parsed: Option<ParsedRunSummary>,
}

fn build_case_metadata(args: BuildCaseMetadataArgs<'_>) -> CaseMetadataV1 {
    let now = unix_seconds();
    let result = args.parsed.map_or_else(
        || CaseResult {
            accept: false,
            days_status: "unknown".to_string(),
            days_error: Some("missing run summary".to_string()),
            trace_discovery_mode: None,
            traces: Vec::new(),
        },
        |parsed| CaseResult {
            accept: parsed.accept,
            days_status: parsed.days_status,
            days_error: parsed.days_error,
            trace_discovery_mode: parsed.trace_mode,
            traces: parsed.traces,
        },
    );

    CaseMetadataV1 {
        version: 1,
        case_id: args.case_id.to_string(),
        created_at_unix_s: now,
        parent: args.parent,
        paths: CasePaths {
            case_dir: args.case_dir.display().to_string(),
            config: args.config_path.display().to_string(),
            log_dir: args.log_dir.display().to_string(),
            run_summary: args.run_summary_path.display().to_string(),
        },
        mutations: args.mutations,
        result,
        coverage: CoverageInfo {
            mode: "stub".to_string(),
            observed: Vec::new(),
            novelty: "unknown".to_string(),
        },
    }
}

fn apply_base_overrides(
    config: &mut toml::Value,
    log_dir: &Path,
    seed: u64,
    force_threading_single: bool,
) -> Vec<Mutation> {
    let mut mutations = Vec::new();
    apply_log_path(config, log_dir);
    mutations.push(Mutation::SetLogPath {
        value: log_dir.display().to_string(),
    });

    let seed_value = (seed & i64::MAX as u64) as i64;
    set_top_level(config, "seed", toml::Value::Integer(seed_value));
    mutations.push(Mutation::SetSeed {
        value: seed_value as u64,
    });

    if force_threading_single {
        set_top_level(
            config,
            "threading",
            toml::Value::String("single".to_string()),
        );
        mutations.push(Mutation::SetThreadingSingle);
    }

    mutations
}

fn apply_log_path(config: &mut toml::Value, log_dir: &Path) {
    set_top_level(
        config,
        "log_path",
        toml::Value::String(log_dir.display().to_string()),
    );
}

fn apply_random_mutations(config: &mut toml::Value, rng: &mut StdRng) -> Vec<Mutation> {
    let mut mutations = Vec::new();
    let mutators: Vec<fn(&mut toml::Value, &mut StdRng) -> Option<Mutation>> = vec![
        mutate_duration,
        mutate_flow_count,
        mutate_initial_delay,
        mutate_arr_dist,
        mutate_pkt_size_dist,
        mutate_switch_port_rate,
        mutate_switch_capacity,
        mutate_shuffle_edges,
    ];

    let target = rng.random_range(1..=3);
    let mut attempts = 0;
    while mutations.len() < target && attempts < mutators.len() * 3 {
        let idx = rng.random_range(0..mutators.len());
        if let Some(mutation) = mutators[idx](config, rng) {
            mutations.push(mutation);
        }
        attempts += 1;
    }
    mutations
}

fn mutate_duration(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    let duration = get_top_level_float(config, "duration")?;
    let factors = [0.25, 0.5, 0.75, 1.5, 2.0];
    let factor = *factors.choose(rng)?;
    let new_value = (duration * factor).max(0.001);
    set_top_level(config, "duration", toml::Value::Float(new_value));
    Some(Mutation::TweakDuration {
        from: duration,
        to: new_value,
    })
}

fn mutate_flow_count(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    let indices = collect_indices(config, "flow_set", "flow_count");
    let idx = *indices.choose(rng)?;
    let flow_count = get_nested_u32(config, "flow_set", idx, "flow_count")?;
    let factors = [0.5, 0.75, 1.5, 2.0];
    let factor = *factors.choose(rng)?;
    let mut new_value = ((flow_count as f64) * factor).round() as u32;
    if new_value == flow_count {
        new_value = flow_count.saturating_add(1).max(1);
    }
    set_nested(
        config,
        "flow_set",
        idx,
        "flow_count",
        toml::Value::Integer(new_value as i64),
    );
    Some(Mutation::TweakFlowCount {
        from: flow_count,
        to: new_value,
    })
}

fn mutate_initial_delay(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    let paths = collect_traffic_paths(config);
    let path = paths.choose(rng)?.clone();
    let traffic = get_traffic_table_mut(config, &path)?;
    let current = traffic.get("initial_delay").and_then(value_to_f64)?;
    let delta = if current.abs() < f64::EPSILON {
        0.001
    } else {
        current.abs() * 0.2
    };
    let new_value = (current + if rng.random_bool(0.5) { delta } else { -delta }).max(0.0);
    traffic.insert("initial_delay".to_string(), toml::Value::Float(new_value));
    Some(Mutation::TweakInitialDelay {
        from: current,
        to: new_value,
    })
}

fn mutate_arr_dist(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    mutate_distribution(config, rng, "arr_dist", 0.0)
}

fn mutate_pkt_size_dist(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    mutate_distribution(config, rng, "pkt_size_dist", 1.0)
}

fn mutate_distribution(
    config: &mut toml::Value,
    rng: &mut StdRng,
    field: &str,
    min_value: f64,
) -> Option<Mutation> {
    let paths = collect_traffic_paths(config);
    let path = paths.choose(rng)?.clone();
    let traffic = get_traffic_table_mut(config, &path)?;
    let dist = traffic.get_mut(field)?.as_table_mut()?;
    let dist_type = dist.get("type")?.as_str()?.to_string();
    let factors = [0.5, 0.75, 1.5, 2.0];
    let factor = *factors.choose(rng)?;

    match dist_type.as_str() {
        "Exp" => {
            let lambda = dist.get("lambda").and_then(value_to_f64)?;
            let new_lambda = (lambda * factor).max(0.000_001);
            dist.insert("lambda".to_string(), toml::Value::Float(new_lambda));
            Some(Mutation::TweakDistribution {
                field: field.to_string(),
                dist_type,
                from: format!("lambda={lambda}"),
                to: format!("lambda={new_lambda}"),
            })
        }
        "Uniform" => {
            let low = dist.get("low").and_then(value_to_f64)?;
            let high = dist.get("high").and_then(value_to_f64)?;
            let mut new_low = (low * factor).max(min_value);
            let mut new_high = (high * factor).max(min_value);
            if new_high < new_low {
                std::mem::swap(&mut new_low, &mut new_high);
            }
            dist.insert("low".to_string(), toml::Value::Float(new_low));
            dist.insert("high".to_string(), toml::Value::Float(new_high));
            Some(Mutation::TweakDistribution {
                field: field.to_string(),
                dist_type,
                from: format!("low={low},high={high}"),
                to: format!("low={new_low},high={new_high}"),
            })
        }
        "DiscreteUniform" => {
            let low = dist.get("low").and_then(value_to_i64)?;
            let high = dist.get("high").and_then(value_to_i64)?;
            let mut new_low = ((low as f64) * factor).round() as i64;
            let mut new_high = ((high as f64) * factor).round() as i64;
            let min_i64 = min_value.max(1.0) as i64;
            if new_low < min_i64 {
                new_low = min_i64;
            }
            if new_high < min_i64 {
                new_high = min_i64;
            }
            if new_high < new_low {
                std::mem::swap(&mut new_low, &mut new_high);
            }
            dist.insert("low".to_string(), toml::Value::Integer(new_low));
            dist.insert("high".to_string(), toml::Value::Integer(new_high));
            Some(Mutation::TweakDistribution {
                field: field.to_string(),
                dist_type,
                from: format!("low={low},high={high}"),
                to: format!("low={new_low},high={new_high}"),
            })
        }
        _ => None,
    }
}

fn mutate_switch_port_rate(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    let table = config.as_table_mut()?;
    let switch = table.get_mut("switch")?.as_table_mut()?;
    let current = switch.get("port_rate").and_then(value_to_f64)?;
    let factors = [0.5, 0.75, 1.25, 1.5, 2.0];
    let factor = *factors.choose(rng)?;
    let new_value = (current * factor).max(1.0);
    switch.insert("port_rate".to_string(), toml::Value::Float(new_value));
    Some(Mutation::TweakSwitchPortRate {
        from: current,
        to: new_value,
    })
}

fn mutate_switch_capacity(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    let table = config.as_table_mut()?;
    let switch = table.get_mut("switch")?.as_table_mut()?;
    let current = switch.get("capacity").and_then(value_to_i64)?;
    let factors = [0.5, 0.75, 1.5, 2.0];
    let factor = *factors.choose(rng)?;
    let mut new_value = ((current as f64) * factor).round() as i64;
    if new_value < 1 {
        new_value = 1;
    }
    switch.insert("capacity".to_string(), toml::Value::Integer(new_value));
    Some(Mutation::TweakSwitchCapacity {
        from: current,
        to: new_value,
    })
}

fn mutate_shuffle_edges(config: &mut toml::Value, rng: &mut StdRng) -> Option<Mutation> {
    let table = config.as_table_mut()?;
    let edges = table.get_mut("edges")?.as_array_mut()?;
    if edges.len() < 2 {
        return None;
    }
    edges.shuffle(rng);
    Some(Mutation::ShuffleEdges)
}

#[derive(Clone, Copy)]
enum TrafficContainer {
    Flow,
    FlowSet,
    Collective,
    CollectiveSet,
}

#[derive(Clone)]
struct TrafficPath {
    container: TrafficContainer,
    index: usize,
}

fn collect_traffic_paths(config: &toml::Value) -> Vec<TrafficPath> {
    let mut paths = Vec::new();
    collect_traffic_paths_for(config, "flow", TrafficContainer::Flow, &mut paths);
    collect_traffic_paths_for(config, "flow_set", TrafficContainer::FlowSet, &mut paths);
    collect_traffic_paths_for(
        config,
        "collective",
        TrafficContainer::Collective,
        &mut paths,
    );
    collect_traffic_paths_for(
        config,
        "collective_set",
        TrafficContainer::CollectiveSet,
        &mut paths,
    );
    paths
}

fn collect_traffic_paths_for(
    config: &toml::Value,
    key: &str,
    container: TrafficContainer,
    out: &mut Vec<TrafficPath>,
) {
    if let Some(arr) = config.get(key).and_then(|v| v.as_array()) {
        for (idx, entry) in arr.iter().enumerate() {
            if entry.get("traffic").is_some() {
                out.push(TrafficPath {
                    container,
                    index: idx,
                });
            }
        }
    }
}

fn get_traffic_table_mut<'a>(
    config: &'a mut toml::Value,
    path: &TrafficPath,
) -> Option<&'a mut toml::value::Table> {
    let table = config.as_table_mut()?;
    let (key, index) = match path.container {
        TrafficContainer::Flow => ("flow", path.index),
        TrafficContainer::FlowSet => ("flow_set", path.index),
        TrafficContainer::Collective => ("collective", path.index),
        TrafficContainer::CollectiveSet => ("collective_set", path.index),
    };
    table
        .get_mut(key)?
        .as_array_mut()?
        .get_mut(index)?
        .as_table_mut()?
        .get_mut("traffic")?
        .as_table_mut()
}

fn collect_indices(config: &toml::Value, array_key: &str, field: &str) -> Vec<usize> {
    let mut indices = Vec::new();
    if let Some(arr) = config.get(array_key).and_then(|v| v.as_array()) {
        for (idx, entry) in arr.iter().enumerate() {
            if entry.get(field).is_some() {
                indices.push(idx);
            }
        }
    }
    indices
}

fn set_top_level(config: &mut toml::Value, key: &str, value: toml::Value) {
    if let Some(table) = config.as_table_mut() {
        table.insert(key.to_string(), value);
    }
}

fn get_top_level_float(config: &toml::Value, key: &str) -> Option<f64> {
    config.get(key).and_then(value_to_f64)
}

fn get_nested_u32(config: &toml::Value, array_key: &str, index: usize, field: &str) -> Option<u32> {
    config
        .get(array_key)?
        .as_array()?
        .get(index)?
        .get(field)
        .and_then(value_to_i64)
        .and_then(|v| u32::try_from(v).ok())
}

fn set_nested(
    config: &mut toml::Value,
    array_key: &str,
    index: usize,
    field: &str,
    value: toml::Value,
) {
    if let Some(arr) = config
        .as_table_mut()
        .and_then(|t| t.get_mut(array_key))
        .and_then(|v| v.as_array_mut())
    {
        if let Some(entry) = arr.get_mut(index).and_then(|v| v.as_table_mut()) {
            entry.insert(field.to_string(), value);
        }
    }
}

fn value_to_f64(value: &toml::Value) -> Option<f64> {
    match value {
        toml::Value::Float(v) => Some(*v),
        toml::Value::Integer(v) => Some(*v as f64),
        _ => None,
    }
}

fn value_to_i64(value: &toml::Value) -> Option<i64> {
    match value {
        toml::Value::Integer(v) => Some(*v),
        toml::Value::Float(v) => Some(*v as i64),
        _ => None,
    }
}

fn read_toml(path: &Path) -> Result<toml::Value, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("Failed to read TOML: {e}"))?;
    toml::from_str(&content).map_err(|e| format!("Failed to parse TOML: {e}"))
}

fn write_toml(path: &Path, value: &toml::Value) -> Result<(), String> {
    let content =
        toml::to_string_pretty(value).map_err(|e| format!("Failed to serialize TOML: {e}"))?;
    fs::write(path, content).map_err(|e| format!("Failed to write TOML: {e}"))
}

fn collect_toml_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    if root.is_file() {
        if root.extension().and_then(|s| s.to_str()) == Some("toml") {
            out.push(root.to_path_buf());
        }
        return Ok(());
    }

    let entries = fs::read_dir(root)
        .map_err(|e| format!("Failed to read directory {}: {e}", root.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {e}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_toml_files(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            out.push(path);
        }
    }
    Ok(())
}

fn sanitize_id(path: &Path) -> String {
    let raw = path.to_string_lossy();
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn detect_required_features(path: &Path) -> Result<Vec<String>, String> {
    let value = read_toml(path)?;
    let mut features = Vec::new();

    if has_flow_type(&value, "dcqcn") {
        features.push("dcqcn".to_string());
    }
    if value
        .get("link")
        .and_then(|v| v.get("mode"))
        .and_then(|v| v.as_str())
        .is_some_and(|m| m.eq_ignore_ascii_case("pfc"))
    {
        features.push("l2_pfc".to_string());
    }

    features.sort();
    features.dedup();
    Ok(features)
}

fn has_flow_type(value: &toml::Value, flow_type: &str) -> bool {
    let check = |arr: &Vec<toml::Value>| {
        arr.iter().any(|flow| {
            flow.get("flow_type")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t.eq_ignore_ascii_case(flow_type))
        })
    };

    if let Some(arr) = value.get("flow").and_then(|v| v.as_array()) {
        if check(arr) {
            return true;
        }
    }
    if let Some(arr) = value.get("flow_set").and_then(|v| v.as_array()) {
        if check(arr) {
            return true;
        }
    }
    false
}

fn resolve_leanguard_run(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }

    let exe =
        std::env::current_exe().map_err(|e| format!("Failed to get current executable: {e}"))?;
    let sibling = exe.with_file_name(if cfg!(windows) {
        "leanguard-run.exe"
    } else {
        "leanguard-run"
    });
    if sibling.exists() {
        return Ok(sibling);
    }

    Ok(PathBuf::from(if cfg!(windows) {
        "leanguard-run.exe"
    } else {
        "leanguard-run"
    }))
}

struct RunOutput {
    stdout: String,
}

fn run_leanguard(
    leanguard_run: &Path,
    checker_dir: &Path,
    config_path: &Path,
    allow_nondeterministic: bool,
) -> Result<RunOutput, String> {
    let mut cmd = Command::new(leanguard_run);
    cmd.arg("--config")
        .arg(config_path)
        .arg("--checker-dir")
        .arg(checker_dir);
    if allow_nondeterministic {
        cmd.arg("--allow-nondeterministic");
    }
    let output = cmd
        .output()
        .map_err(|e| format!("Failed to run leanguard-run: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("leanguard-run failed: {stderr}"));
    }

    Ok(RunOutput {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
    })
}

fn extract_json(stdout: &str) -> Result<String, String> {
    let start = stdout
        .find('{')
        .ok_or_else(|| "Missing JSON start".to_string())?;
    let end = stdout
        .rfind('}')
        .ok_or_else(|| "Missing JSON end".to_string())?;
    if end <= start {
        return Err("Invalid JSON boundaries".to_string());
    }
    Ok(stdout[start..=end].to_string())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let content = serde_json::to_string_pretty(value)
        .map_err(|e| format!("Failed to serialize JSON: {e}"))?;
    fs::write(path, content).map_err(|e| format!("Failed to write JSON: {e}"))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn default_rng_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn shrink_duration(config: &toml::Value) -> Option<(toml::Value, Mutation)> {
    let duration = config.get("duration").and_then(value_to_f64)?;
    if duration <= 0.001 {
        return None;
    }
    let mut candidate = config.clone();
    let new_value = (duration * 0.5).max(0.001);
    if let Some(table) = candidate.as_table_mut() {
        table.insert("duration".to_string(), toml::Value::Float(new_value));
    }
    Some((
        candidate,
        Mutation::TweakDuration {
            from: duration,
            to: new_value,
        },
    ))
}

fn shrink_flow_count(config: &toml::Value) -> Option<(toml::Value, Mutation)> {
    let indices = collect_indices(config, "flow_set", "flow_count");
    let idx = indices.first().copied()?;
    let flow_count = get_nested_u32(config, "flow_set", idx, "flow_count")?;
    if flow_count <= 1 {
        return None;
    }
    let new_value = (flow_count / 2).max(1);
    let mut candidate = config.clone();
    set_nested(
        &mut candidate,
        "flow_set",
        idx,
        "flow_count",
        toml::Value::Integer(new_value as i64),
    );
    Some((
        candidate,
        Mutation::TweakFlowCount {
            from: flow_count,
            to: new_value,
        },
    ))
}

fn shrink_traffic_size_or_duration(config: &toml::Value) -> Option<(toml::Value, Mutation)> {
    let mut candidate = config.clone();
    let paths = collect_traffic_paths(config);
    let path = paths.first()?.clone();
    let traffic = get_traffic_table_mut(&mut candidate, &path)?;
    if let Some(size) = traffic.get("size").and_then(value_to_i64) {
        let new_size = (size / 2).max(1);
        traffic.insert("size".to_string(), toml::Value::Integer(new_size));
        return Some((
            candidate,
            Mutation::TweakDistribution {
                field: "size".to_string(),
                dist_type: "size".to_string(),
                from: size.to_string(),
                to: new_size.to_string(),
            },
        ));
    }
    if let Some(duration) = traffic.get("duration").and_then(value_to_f64) {
        let new_value = (duration * 0.5).max(0.001);
        traffic.insert("duration".to_string(), toml::Value::Float(new_value));
        return Some((
            candidate,
            Mutation::TweakDistribution {
                field: "duration".to_string(),
                dist_type: "duration".to_string(),
                from: duration.to_string(),
                to: new_value.to_string(),
            },
        ));
    }
    None
}

fn shrink_uniform_ranges(config: &toml::Value) -> Option<(toml::Value, Mutation)> {
    let mut candidate = config.clone();
    let paths = collect_traffic_paths(config);
    let path = paths.first()?.clone();
    let traffic = get_traffic_table_mut(&mut candidate, &path)?;
    for field in ["arr_dist", "pkt_size_dist"] {
        if let Some(dist) = traffic.get_mut(field).and_then(|v| v.as_table_mut()) {
            let dist_type = dist.get("type").and_then(|v| v.as_str())?;
            if dist_type == "Uniform" {
                let low = dist.get("low").and_then(value_to_f64)?;
                let high = dist.get("high").and_then(value_to_f64)?;
                if (high - low).abs() < f64::EPSILON {
                    continue;
                }
                let mid = (low + high) / 2.0;
                dist.insert("low".to_string(), toml::Value::Float(mid));
                dist.insert("high".to_string(), toml::Value::Float(mid));
                return Some((
                    candidate,
                    Mutation::TweakDistribution {
                        field: field.to_string(),
                        dist_type: "Uniform".to_string(),
                        from: format!("low={low},high={high}"),
                        to: format!("low={mid},high={mid}"),
                    },
                ));
            }
        }
    }
    None
}
