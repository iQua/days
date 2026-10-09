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

use days::scenario::{LEGACY_ENGINE_ROOT_KEYS, compile_config};
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

/// `text` with `entry` added to the inline table on the first line assigning `key`.
fn add_to_inline(text: &str, key: &str, entry: &str) -> String {
    let mut out = String::new();
    let mut done = false;
    for line in text.lines() {
        if !done && line.trim_start().starts_with(&format!("{key} =")) {
            let close = line.rfind('}').expect("an inline table");
            out.push_str(&format!("{}, {entry} {}", &line[..close], &line[close..]));
            done = true;
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    assert!(done, "no `{key}` line");
    out
}

/// Fix round 1 (review M1): the inline distribution tables refuse an unknown key and a key of
/// another distribution type, in both a `[[flow]]` and a `[[flow_set]]` traffic table, naming the
/// key, the distribution and its table.
#[test]
fn every_distribution_refuses_unknown_and_wrong_type_keys() {
    let mut accepted = Vec::new();
    for (path, table) in [
        ("configs/p14/dcqcn_t26.toml", "[flow.traffic]"),
        (
            "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
            "[flow_set.traffic]",
        ),
    ] {
        let base = std::fs::read_to_string(repository(path)).expect("read config");
        lower_text(&base).unwrap_or_else(|error| panic!("{path} lowers: {error}"));
        for (distribution, entry, key) in [
            ("arr_dist", "hgih = 3", "hgih"),
            ("arr_dist", "lambda = 2", "lambda"),
            ("pkt_size_dist", "bogus = 1", "bogus"),
            ("pkt_size_dist", "lambda = 2", "lambda"),
        ] {
            let label = format!("`{key}` in `{distribution}` (in `{table}`)");
            match lower_text(&add_to_inline(&base, distribution, entry)) {
                Ok(_) => accepted.push(format!("{label}: accepted")),
                Err(error) => {
                    if !error.contains(&format!("unknown field `{key}`"))
                        || !error.contains(&format!("in `{distribution}`"))
                        || !error.contains(&format!("(in `{table}`)"))
                    {
                        accepted.push(format!("{label}: refused without naming it: {error}"));
                    }
                }
            }
        }
    }
    assert!(accepted.is_empty(), "{}", accepted.join("\n"));
}

/// The entries of the inline table on the first `key = { ... }` line of `text`, and that line's
/// indentation.
fn inline_entries<'a>(text: &'a str, key: &str) -> (&'a str, Vec<&'a str>) {
    let line = text
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{key} =")))
        .unwrap_or_else(|| panic!("no `{key}` line"));
    let indent = &line[..line.len() - line.trim_start().len()];
    let open = line.find('{').expect("an inline table");
    let close = line.rfind('}').expect("an inline table");
    let entries = line[open + 1..close].split(',').map(str::trim).collect();
    (indent, entries)
}

/// `text` with `key`'s inline table written as dotted keys (`key.type = ...`) in place.
fn to_dotted(text: &str, key: &str) -> String {
    let (indent, entries) = inline_entries(text, key);
    let mut out = String::new();
    let mut done = false;
    for line in text.lines() {
        if !done && line.trim_start().starts_with(&format!("{key} =")) {
            for entry in &entries {
                out.push_str(&format!("{indent}{key}.{entry}\n"));
            }
            done = true;
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// `text` with `key`'s inline table moved to a `[<table>.<key>]` sub-table, written just before
/// the first header after `table`'s.
fn to_subtable(text: &str, table: &str, key: &str) -> String {
    let (indent, entries) = inline_entries(text, key);
    let path = table.trim_matches(['[', ']']);
    let mut sub = format!("{indent}[{path}.{key}]\n");
    for entry in &entries {
        sub.push_str(&format!("{indent}{entry}\n"));
    }
    let mut out = String::new();
    let (mut inside, mut done) = (false, false);
    for line in text.lines() {
        let trimmed = line.trim();
        if inside && !done && trimmed.starts_with('[') {
            out.push_str(&sub);
            done = true;
        }
        if trimmed == table {
            inside = true;
        }
        if !(inside && trimmed.starts_with(&format!("{key} ="))) {
            out.push_str(line);
            out.push('\n');
        }
    }
    if !done {
        out.push_str(&sub);
    }
    out
}

/// Fix round 2 (re-review F1, ruling option (a)): a distribution written as a sub-table or as
/// dotted keys is refused on every traffic kind, as PacketDistribution traffic already refused
/// it, with one message naming the key and the table.
#[test]
fn every_distribution_refuses_sub_tables_and_dotted_keys() {
    let mut wrong = Vec::new();
    for (path, table) in [
        (
            "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
            "[flow_set.traffic]",
        ),
        ("configs/p14/dcqcn_t26.toml", "[flow.traffic]"),
        (
            "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
            "[flow_set.traffic]",
        ),
        ("configs/p15/roce_ring_lossy.toml", "[collective.traffic]"),
    ] {
        let base = std::fs::read_to_string(repository(path)).expect("read config");
        lower_text(&base).unwrap_or_else(|error| panic!("{path} lowers: {error}"));
        for distribution in ["arr_dist", "pkt_size_dist"] {
            let sub_table = format!("`[{}.{distribution}]`", table.trim_matches(['[', ']']));
            for (form, text, named) in [
                (
                    "dotted keys",
                    to_dotted(&base, distribution),
                    format!("`{table}`"),
                ),
                (
                    "sub-table",
                    to_subtable(&base, table, distribution),
                    sub_table,
                ),
            ] {
                // Valid TOML: only Days AGO's executor-distribution rule refuses it.
                text.parse::<toml::Table>()
                    .unwrap_or_else(|error| panic!("{path} {form}: {error}\n{text}"));
                let expected = format!(
                    "invalid scenario: executor distributions must use an inline TOML table in `{distribution}` (in {named})"
                );
                let result = lower_text(&text).map(|_| ());
                if result.as_ref().err() != Some(&expected) {
                    wrong.push(format!(
                        "{path} `{distribution}` as {form}: {result:?}, expected {expected:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// `text` with the first `key = { ... }` line replaced by `write(indent, entries)`.
fn rewrite_inline(text: &str, key: &str, write: impl Fn(&str, &[&str]) -> String) -> String {
    let (indent, entries) = inline_entries(text, key);
    let mut out = String::new();
    let mut done = false;
    for line in text.lines() {
        if !done && line.trim_start().starts_with(&format!("{key} =")) {
            out.push_str(&write(indent, &entries));
            done = true;
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

/// Fix round 3 (re-review G1 and G2, orchestrator ruling): an executor distribution is a one-line
/// inline table without comments or a trailing comma. TOML 1.1 allows all three in an inline
/// table, but the exact reader splits the text on `,` and `=`, so a commented-out line would
/// override the live values. Each form is refused, on every traffic kind, naming the key and the
/// table.
#[test]
fn every_distribution_refuses_comments_newlines_and_trailing_commas() {
    let mut wrong = Vec::new();
    for (path, table) in [
        (
            "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
            "[flow_set.traffic]",
        ),
        ("configs/p14/dcqcn_t26.toml", "[flow.traffic]"),
        (
            "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
            "[flow_set.traffic]",
        ),
        ("configs/p15/roce_ring_lossy.toml", "[collective.traffic]"),
    ] {
        let base = std::fs::read_to_string(repository(path)).expect("read config");
        lower_text(&base).unwrap_or_else(|error| panic!("{path} lowers: {error}"));
        for distribution in ["arr_dist", "pkt_size_dist"] {
            let forms: [(&str, String); 4] = [
                (
                    "a commented-out line",
                    rewrite_inline(&base, distribution, |indent, entries| {
                        let live = entries.join(", ");
                        let old = entries
                            .iter()
                            .map(|entry| match entry.split_once('=') {
                                Some((key, _)) if key.trim() != "type" => format!("{key}= 7"),
                                _ => (*entry).to_owned(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!(
                            "{indent}{distribution} = {{\n{indent}  {live}\n{indent}  # {old}\n{indent}}}"
                        )
                    }),
                ),
                (
                    "several lines",
                    rewrite_inline(&base, distribution, |indent, entries| {
                        let lines = entries
                            .iter()
                            .map(|entry| format!("{indent}  {entry}"))
                            .collect::<Vec<_>>()
                            .join(",\n");
                        format!("{indent}{distribution} = {{\n{lines}\n{indent}}}")
                    }),
                ),
                (
                    "several lines and a trailing comma",
                    rewrite_inline(&base, distribution, |indent, entries| {
                        let lines = entries
                            .iter()
                            .map(|entry| format!("{indent}  {entry},\n"))
                            .collect::<String>();
                        format!("{indent}{distribution} = {{\n{lines}{indent}}}")
                    }),
                ),
                (
                    "one line and a trailing comma",
                    rewrite_inline(&base, distribution, |indent, entries| {
                        format!("{indent}{distribution} = {{ {}, }}", entries.join(", "))
                    }),
                ),
            ];
            for (form, text) in forms {
                // Valid TOML 1.1: only Days AGO's executor-distribution rule refuses it.
                text.parse::<toml::Table>()
                    .unwrap_or_else(|error| panic!("{path} {form}: {error}\n{text}"));
                let expected = format!(
                    "invalid scenario: executor distributions must use a one-line inline TOML table without comments or a trailing comma in `{distribution}` (in `{table}`)"
                );
                let result = lower_text(&text).map(|_| ());
                if result.as_ref().err() != Some(&expected) {
                    wrong.push(format!(
                        "{path} `{distribution}` with {form}: {result:?}, expected {expected:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
