//! P16 a2aset part 2 (user ruling, Oct 8): Days AGO refuses unknown configuration keys at every
//! level, naming the key and its table. The root accepts its sibling parsers' keys (`topology`,
//! `edges`, `hosts`) and the legacy engine's root keys by exact name
//! (`days::scenario::LEGACY_ENGINE_ROOT_KEYS`), which it ignores.
//!
//! Each case takes a scenario that lowers (a `configs/` file or a small inline one), checks that
//! it lowers, then adds one misspelled or unknown key to one table and requires the lowering to
//! refuse it with `unknown field `<key>`` and the table's header.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;

/// The legacy engine's root keys the Days AGO root accepts by name (`legacy/src/config.rs`).
const LEGACY_ENGINE_ROOT_KEYS: [&str; 12] = [
    "ui_interval",
    "threading",
    "num_threads",
    "hot_workers",
    "concurrency_level",
    "log_path",
    "csv_logging",
    "report_interval",
    "mailbox_capacity",
    "legacy_e5_metrics",
    "model_host_attachment",
    "app_source",
];
use days_executor::SimulationImage;

fn repository(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn lower_text(config: &str) -> Result<SimulationImage, String> {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p16-unknown-keys-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, config).expect("write scenario");
    let image = compile_config(&path).map_err(|error| error.to_string());
    std::fs::remove_file(&path).expect("remove scenario");
    image
}

/// `text` with `line` inserted after the first line that is exactly `header` (trimmed), or at the
/// top for the root table.
fn insert(text: &str, header: Option<&str>, line: &str) -> String {
    let Some(header) = header else {
        return format!("{line}\n{text}");
    };
    let mut out = String::new();
    let mut done = false;
    for current in text.lines() {
        out.push_str(current);
        out.push('\n');
        if !done && current.trim() == header {
            out.push_str(line);
            out.push('\n');
            done = true;
        }
    }
    assert!(done, "header {header} not found");
    out
}

/// A two-member TCP all-to-all `[[collective_set]]` on four hosts.
const SET: &str = r#"
seed = 26
edges = [[0, 4], [1, 4], [2, 4], [3, 4]]
hosts = [0, 1, 2, 3]
duration = 0.05

[switch]
port_rate = 8000000000
capacity = 200
discipline = "FIFO"
drop = "TailDrop"

[[collective_set]]
collective_type = "AllToAll"
collective_count = 2
flow_type = "TCP"
flow_count = 2
sources = [[0, 1], [2, 3]]

[collective_set.traffic]
initial_delay = 0.0
size = 8000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 500, high = 500 }

[collective_set.traffic.tcp]
cc_algorithm = "TCPReno"
"#;

/// `(base scenario text, table header or None for the root, inserted line, unknown key)`.
fn cases() -> Vec<(String, Option<&'static str>, &'static str, &'static str)> {
    let file = |path: &str| std::fs::read_to_string(repository(path)).expect("read config");
    let t26 = file("configs/p14/dcqcn_t26.toml");
    let pfc = file("configs/p14/dcqcn_t26_pfc.toml");
    let tiers = file("configs/benchmarks/evaluation/f_het_k8_tiered_delays.toml");
    let routing = file("configs/benchmarks/evaluation/e6_cbr_k32_load_01.toml");
    let cubic = file("configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml");
    let roce = file("configs/p15/roce_ring_lossy.toml");
    let compute = file("configs/p15/roce_allgather_compute_lossy.toml");
    let fattree = file("configs/fattree.toml");
    vec![
        (t26.clone(), None, "sead = 1", "sead"),
        (t26.clone(), Some("[switch]"), "capacty = 3", "capacty"),
        (t26.clone(), Some("[link]"), "mod = \"Bulk\"", "mod"),
        (pfc, Some("[link.pfc]"), "xof = [1]", "xof"),
        (
            tiers,
            Some("[link.propagation_tiers]"),
            "host_to_core_ns = 5",
            "host_to_core_ns",
        ),
        (
            routing,
            Some("[routing]"),
            "polcy = \"ShortestPath\"",
            "polcy",
        ),
        (t26.clone(), Some("[[flow]]"), "prority = 1", "prority"),
        (t26, Some("[flow.traffic]"), "sise = 1000", "sise"),
        (
            cubic.clone(),
            Some("[[flow_set]]"),
            "flow_cnt = 1",
            "flow_cnt",
        ),
        (
            cubic.clone(),
            Some("[flow_set.traffic.tcp]"),
            "ecm = true",
            "ecm",
        ),
        (
            cubic,
            Some("[flow_set.traffic.tcp.cubic]"),
            "bta = 0.7",
            "bta",
        ),
        (roce.clone(), Some("[[collective]]"), "afer = \"x\"", "afer"),
        (roce, Some("[collective.traffic]"), "sise = 1", "sise"),
        (compute, Some("[[compute]]"), "afer = \"x\"", "afer"),
        (
            SET.to_owned(),
            Some("[[collective_set]]"),
            "name = \"set\"",
            "name",
        ),
        (
            SET.to_owned(),
            Some("[[collective_set]]"),
            "after = \"nothing\"",
            "after",
        ),
        (
            SET.to_owned(),
            Some("[[collective_set]]"),
            "stream = 1",
            "stream",
        ),
        (
            SET.to_owned(),
            Some("[[collective_set]]"),
            "channels = [[0, 1]]",
            "channels",
        ),
        (
            SET.to_owned(),
            Some("[[collective_set]]"),
            "chunk = \"UniformFloor\"",
            "chunk",
        ),
        (
            format!("{SET}\n[collective_set.alltoall]\nseed = 1\n"),
            Some("[collective_set.alltoall]"),
            "matrix = 1",
            "alltoall",
        ),
        (
            SET.to_owned(),
            Some("[collective_set.traffic]"),
            "sise = 1",
            "sise",
        ),
        (
            SET.to_owned(),
            Some("[collective_set.traffic.tcp]"),
            "ecm = true",
            "ecm",
        ),
        (
            fattree.clone(),
            Some("[topology]"),
            "categry = \"FatTree\"",
            "categry",
        ),
        (
            fattree,
            Some("[topology.fat_tree]"),
            "hosts_per_edg = 1",
            "hosts_per_edg",
        ),
    ]
}

#[test]
fn every_days_ago_table_refuses_an_unknown_key() {
    let mut accepted = Vec::new();
    for (base, header, line, key) in cases() {
        // `[collective_set.alltoall]` itself is the unknown key there: its base does not lower.
        if key != "alltoall" {
            lower_text(&base).unwrap_or_else(|error| {
                panic!("the base of {key} in {header:?} must lower: {error}")
            });
        }
        let table = match (header, key) {
            (_, "alltoall") => "in `[collective_set.alltoall]`".to_owned(),
            (Some(header), _) => format!("in `{header}`"),
            (None, _) => "in the root table".to_owned(),
        };
        match lower_text(&insert(&base, header, line)) {
            Ok(_) => accepted.push(format!("`{key}` {table}: accepted")),
            Err(error) => {
                if !error.contains(&format!("unknown field `{key}`")) || !error.contains(&table) {
                    accepted.push(format!(
                        "`{key}` {table}: refused without naming it: {error}"
                    ));
                }
            }
        }
    }
    assert!(accepted.is_empty(), "{}", accepted.join("\n"));
}

/// The topology tables a lowering reads, through the topology builder.
#[test]
fn every_topology_table_refuses_an_unknown_key() {
    let mut accepted = Vec::new();
    for (path, header, line, key) in [
        (
            "configs/torus.toml",
            "[topology.torus]",
            "dimension = 2",
            "dimension",
        ),
        (
            "configs/benchmarks/evaluation/f_topo_dragonfly_g33.toml",
            "[topology.dragonfly]",
            "routers = 2",
            "routers",
        ),
    ] {
        let base = std::fs::read_to_string(repository(path)).expect("read config");
        days::topos::build::build_graph_with_profile_from_str(&base)
            .unwrap_or_else(|error| panic!("{path} builds: {error}"));
        match days::topos::build::build_graph_with_profile_from_str(&insert(
            &base,
            Some(header),
            line,
        )) {
            Ok(_) => accepted.push(format!("`{key}` in `{header}`: accepted")),
            Err(error) => {
                let error = error.to_string();
                if !error.contains(&format!("unknown field `{key}`"))
                    || !error.contains(&format!("in `{header}`"))
                {
                    accepted.push(format!("`{key}` in `{header}`: {error}"));
                }
            }
        }
    }
    assert!(accepted.is_empty(), "{}", accepted.join("\n"));
}

/// An AICB scenario's `[topology]` table, read by the adapter.
#[test]
fn an_aicb_topology_refuses_an_unknown_key() {
    let fixtures = repository("tests/fixtures/aicb");
    let directory = tempfile::TempDir::new().expect("temp dir");
    for file in ["reduced-dense-w32-tp8-pp2.txt", "SimAI.conf"] {
        std::fs::copy(fixtures.join(file), directory.path().join(file)).unwrap();
    }
    let base = std::fs::read_to_string(fixtures.join("reduced-dense-simai.toml")).unwrap();
    for (text, key) in [
        (base.clone(), None),
        (
            insert(&base, Some("[topology]"), "categry = \"SpectrumX\""),
            Some("categry"),
        ),
        (insert(&base, None, "sead = 1"), Some("sead")),
    ] {
        let path = directory.path().join("scenario.toml");
        std::fs::write(&path, text).unwrap();
        match (compile_config(&path), key) {
            (Ok(_), None) => {}
            (Err(error), Some(key)) => {
                let error = error.to_string();
                assert!(
                    error.contains(&format!("unknown field `{key}`")),
                    "{key}: {error}"
                );
            }
            (Ok(_), Some(key)) => panic!("`{key}` must be refused in an AICB scenario"),
            (Err(error), None) => panic!("the AICB base lowers: {error}"),
        }
    }
}

/// The root accepts each legacy-engine key by name and ignores it: the image is the base's.
#[test]
fn the_root_accepts_and_ignores_the_legacy_engine_keys() {
    let base = std::fs::read_to_string(repository("configs/p14/dcqcn_t26.toml")).unwrap();
    let expected = lower_text(&base).expect("the base lowers");
    assert_eq!(LEGACY_ENGINE_ROOT_KEYS.len(), 12);
    for key in LEGACY_ENGINE_ROOT_KEYS {
        let image = lower_text(&format!("{key} = 1\n{base}"))
            .unwrap_or_else(|error| panic!("`{key}` must be accepted: {error}"));
        assert_eq!(image, expected, "`{key}` must not change the image");
    }
}
