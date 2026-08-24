use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days::topos::build::build_graph;
use days_executor::{
    CpuConfig, FlowGeneratorKind, ObservationMode, run_cpu, run_cpu_with_observations, run_scalar,
    run_scalar_with_observations,
};
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, run_cuda, run_cuda_with_observations};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{MetalConfig, run_metal, run_metal_with_observations};
use days_legacy::flows::flow::Flow;

const DIRECTORY: &str = "configs/benchmarks/tcp";
const FLOW_BYTES: i64 = 23_360;
const MSS_BYTES: i64 = 1460;

fn assert_result_eq(
    label: &str,
    actual: &days_executor::RunResult,
    expected: &days_executor::RunResult,
) {
    let actual_diagnostics = actual
        .diagnostics
        .as_ref()
        .expect("full reference observation retains diagnostics");
    let expected_diagnostics = expected
        .diagnostics
        .as_ref()
        .expect("full reference observation retains diagnostics");
    for (index, (actual, expected)) in actual_diagnostics
        .tcp_transitions
        .iter()
        .zip(&expected_diagnostics.tcp_transitions)
        .enumerate()
    {
        assert_eq!(actual, expected, "{label} TCP transition {index}");
    }
    assert_eq!(
        actual_diagnostics.tcp_transitions.len(),
        expected_diagnostics.tcp_transitions.len(),
        "{label} TCP transition count"
    );
    assert_eq!(
        actual_diagnostics, expected_diagnostics,
        "{label} diagnostics"
    );
    assert_eq!(
        actual.observed_packets.len(),
        expected.observed_packets.len(),
        "{label} observed-packet count"
    );
    for (index, (actual, expected)) in actual
        .observed_packets
        .iter()
        .zip(&expected.observed_packets)
        .enumerate()
    {
        assert_eq!(actual, expected, "{label} observed packet {index}");
    }
    assert_eq!(actual.host_states.len(), expected.host_states.len());
    for (index, (actual, expected)) in actual
        .host_states
        .iter()
        .zip(&expected.host_states)
        .enumerate()
    {
        assert_eq!(actual, expected, "{label} host state {index}");
    }
    assert_eq!(actual.switch_states.len(), expected.switch_states.len());
    for (index, (actual, expected)) in actual
        .switch_states
        .iter()
        .zip(&expected.switch_states)
        .enumerate()
    {
        assert_eq!(actual, expected, "{label} switch state {index}");
    }
    assert_eq!(actual.summary, expected.summary, "{label} summary");
    assert_eq!(
        actual.resident_packets, expected.resident_packets,
        "{label} resident packets"
    );
    assert_eq!(
        actual.observed_packets, expected.observed_packets,
        "{label} observed packets"
    );
    assert_eq!(actual.departures, expected.departures, "{label} departures");
    assert_eq!(actual.arrivals, expected.arrivals, "{label} arrivals");
    assert_eq!(
        actual.pending_events, expected.pending_events,
        "{label} pending events"
    );
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn assert_device_full_result_eq(
    label: &str,
    actual: &days_executor::RunResult,
    scalar: &days_executor::RunResult,
) {
    assert!(
        scalar.diagnostics.is_some(),
        "{label} scalar Full diagnostics must be present"
    );
    assert!(
        actual.diagnostics.is_none(),
        "{label} device Full diagnostics must be absent"
    );
    let mut expected = scalar.clone();
    expected.diagnostics = None;
    assert_eq!(actual, &expected, "{label}");
}

#[derive(Clone, Copy)]
struct Corpus {
    file: &'static str,
    k: i64,
    hosts: usize,
    algorithm: &'static str,
}

/// Formal k16/k32 ladder points, whose manifests are pinned by
/// `canonical_tcp_corpus_manifests_are_one_flow_per_host_at_k16_and_k32`.
const CORPORA: [Corpus; 4] = [
    Corpus {
        file: "fattree_k16_tcp_reno_f1024.toml",
        k: 16,
        hosts: 1024,
        algorithm: "TCPReno",
    },
    Corpus {
        file: "fattree_k16_tcp_cubic_f1024.toml",
        k: 16,
        hosts: 1024,
        algorithm: "CUBIC",
    },
    Corpus {
        file: "fattree_k32_tcp_reno_f8192.toml",
        k: 32,
        hosts: 8192,
        algorithm: "TCPReno",
    },
    Corpus {
        file: "fattree_k32_tcp_cubic_f8192.toml",
        k: 32,
        hosts: 8192,
        algorithm: "CUBIC",
    },
];

/// The k4 harness smoke fixture.
///
/// It is deliberately not a ladder point (`configs/benchmarks/tcp/t24-corpus-manifest.md`), which
/// is why it stays out of `CORPORA` and out of the k16/k32 manifest assertions. It is still a real
/// canonical TCP image, so it is a full member of the lowering and four-backend execution
/// campaigns below.
const SMOKE: Corpus = Corpus {
    file: "fattree_k4_tcp_cubic_f16_smoke.toml",
    k: 4,
    hosts: 16,
    algorithm: "CUBIC",
};

/// Every corpus the lowering and four-backend byte-identity campaigns execute: the smoke fixture
/// first, then the ladder. `T24_CORPUS` selects one member of this list by file name.
const CAMPAIGN_CORPORA: [Corpus; 5] = [SMOKE, CORPORA[0], CORPORA[1], CORPORA[2], CORPORA[3]];

/// Campaign members selected by an optional `T24_CORPUS` file-name filter.
///
/// Pure so the selection semantics are testable without mutating process environment.
fn select_campaign_corpora(filter: Option<&str>) -> Vec<Corpus> {
    CAMPAIGN_CORPORA
        .into_iter()
        .filter(|corpus| filter.is_none_or(|filter| filter == corpus.file))
        .collect()
}

/// Campaign members selected by the ambient `T24_CORPUS` filter, guaranteed non-empty.
///
/// A filter that matches no campaign member is a harness error, never an empty pass: a campaign
/// that compares nothing must not be able to report a green row (T20g review round 1, M1).
fn selected_campaign_corpora() -> Vec<Corpus> {
    let filter = std::env::var("T24_CORPUS").ok();
    let selected = select_campaign_corpora(filter.as_deref());
    assert!(
        !selected.is_empty(),
        "T24_CORPUS={} matched no campaign corpus; valid names: {}",
        filter.as_deref().unwrap_or("<unset>"),
        CAMPAIGN_CORPORA.map(|corpus| corpus.file).join(", ")
    );
    selected
}

fn path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(DIRECTORY)
        .join(file)
}

fn table(path: &Path) -> toml::Table {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        .parse()
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

#[test]
fn canonical_tcp_corpus_manifests_are_one_flow_per_host_at_k16_and_k32() {
    for corpus in CORPORA {
        let path = path(corpus.file);
        let config = table(&path);
        let expected_hosts = usize::try_from(corpus.k.pow(3) / 4).unwrap();
        assert_eq!(corpus.hosts, expected_hosts);
        assert_eq!(config["duration"].as_float(), Some(0.001152));
        assert_eq!(config["topology"]["category"].as_str(), Some("FatTree"));
        assert_eq!(
            config["topology"]["fat_tree"]["k"].as_integer(),
            Some(corpus.k)
        );
        assert_eq!(
            config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(corpus.k / 2)
        );
        assert_eq!(
            config["switch"]["port_rate"].as_integer(),
            Some(100_000_000_000)
        );
        assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
        assert_eq!(config["link"]["propagation_ns"].as_integer(), Some(1000));

        let sets = config["flow_set"]
            .as_array()
            .expect("corpus must have one flow set");
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0]["flow_type"].as_str(), Some("TCP"));
        assert_eq!(
            sets[0]["flow_count"].as_integer(),
            Some(i64::try_from(corpus.hosts).unwrap())
        );
        let traffic = &sets[0]["traffic"];
        assert_eq!(traffic["size"].as_integer(), Some(FLOW_BYTES));
        assert_eq!(
            traffic["pkt_size_dist"]["low"].as_integer(),
            Some(MSS_BYTES)
        );
        assert_eq!(
            traffic["pkt_size_dist"]["high"].as_integer(),
            Some(MSS_BYTES)
        );
        assert_eq!(
            traffic["tcp"]["cc_algorithm"].as_str(),
            Some(corpus.algorithm)
        );
        if corpus.algorithm == "CUBIC" {
            assert_eq!(traffic["tcp"]["cubic"]["beta"].as_float(), Some(0.7));
            assert_eq!(traffic["tcp"]["cubic"]["c"].as_float(), Some(0.4));
            assert_eq!(
                traffic["tcp"]["cubic"]["fast_convergence"].as_bool(),
                Some(true)
            );
        }

        let path_string = path.to_str().expect("fixture path must be UTF-8");
        let (_, hosts) = build_graph(path_string).expect("canonical fat tree should build");
        assert_eq!(hosts.len(), corpus.hosts);
        let flows = Flow::flows_from_config_with_attachments(path_string, &hosts);
        assert_eq!(flows.len(), corpus.hosts);
        assert_eq!(
            flows
                .iter()
                .map(|flow| flow.source_host)
                .collect::<BTreeSet<_>>()
                .len(),
            corpus.hosts,
            "each canonical host must source exactly one flow"
        );
        assert_eq!(
            flows
                .iter()
                .map(|flow| flow.sink_host)
                .collect::<BTreeSet<_>>()
                .len(),
            corpus.hosts,
            "the canonical half-rotation must target every host exactly once"
        );
        assert!(
            flows.iter().all(|flow| flow.source_host != flow.sink_host),
            "canonical corpus must not contain self traffic"
        );
    }
}

#[test]
fn tcp_smoke_fixture_lowers_and_is_scalar_cpu_byte_identical() {
    let image = compile_config(path("fattree_k4_tcp_cubic_f16_smoke.toml"))
        .expect("TCP smoke fixture should lower");
    assert_eq!(image.flows.len(), 16);
    assert_eq!(
        image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Tcp(_)))
            .count(),
        16
    );
    let scalar = run_scalar(&image, None).expect("scalar smoke should run");
    let cpu = run_cpu(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
    )
    .expect("CPU smoke should run")
    .result;
    assert_eq!(cpu, scalar);

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    {
        let metal = run_metal(&image, None, MetalConfig::default())
            .expect("Metal TCP corpus smoke should run");
        assert_eq!(metal.result, scalar);
    }

    #[cfg(feature = "cuda")]
    {
        let cuda = run_cuda(&image, None, CudaConfig::default())
            .expect("CUDA TCP corpus smoke should run");
        assert_eq!(cuda.result, scalar);
    }
}

#[test]
fn campaign_corpora_cover_the_ladder_and_the_smoke_fixture() {
    assert_eq!(CAMPAIGN_CORPORA.len(), CORPORA.len() + 1);
    for corpus in CORPORA {
        assert!(
            CAMPAIGN_CORPORA
                .iter()
                .any(|member| member.file == corpus.file),
            "ladder corpus {} must be a campaign member",
            corpus.file
        );
    }
    assert!(
        CAMPAIGN_CORPORA
            .iter()
            .any(|member| member.file == SMOKE.file),
        "the smoke fixture must be a campaign member"
    );
    for corpus in CAMPAIGN_CORPORA {
        assert!(
            path(corpus.file).is_file(),
            "campaign corpus {} must exist on disk",
            corpus.file
        );
    }
}

#[test]
fn a_campaign_filter_matching_no_corpus_selects_nothing() {
    assert_eq!(
        select_campaign_corpora(None).len(),
        CAMPAIGN_CORPORA.len(),
        "an absent filter runs the whole campaign"
    );
    assert_eq!(
        select_campaign_corpora(Some(SMOKE.file))
            .into_iter()
            .map(|corpus| corpus.file)
            .collect::<Vec<_>>(),
        vec![SMOKE.file],
        "an exact file name selects exactly that corpus"
    );
    for miss in [
        "fattree_k4_tcp_cubic_f16_smoke",
        "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
        "fattree_k8_tcp_reno_f128.toml",
    ] {
        assert!(
            select_campaign_corpora(Some(miss)).is_empty(),
            "{miss} must not select any corpus"
        );
    }
}

#[test]
#[ignore = "explicit T24 corpus lowering; run before device conformance campaigns"]
fn full_tcp_corpora_lower_to_exact_tcp_images() {
    let mut lowered = Vec::new();
    for corpus in CAMPAIGN_CORPORA {
        let image = compile_config(path(corpus.file))
            .unwrap_or_else(|error| panic!("{} should lower: {error}", corpus.file));
        assert_eq!(image.flows.len(), corpus.hosts);
        assert_eq!(image.initial_packets.len(), corpus.hosts);
        assert_eq!(
            image
                .flows
                .iter()
                .map(|flow| flow.source)
                .collect::<BTreeSet<_>>()
                .len(),
            corpus.hosts,
            "{} executor image must source one flow from every host",
            corpus.file
        );
        assert_eq!(
            image
                .flows
                .iter()
                .map(|flow| flow.target)
                .collect::<BTreeSet<_>>()
                .len(),
            corpus.hosts,
            "{} executor image must target every host exactly once",
            corpus.file
        );
        assert_eq!(
            image
                .host_states
                .iter()
                .flat_map(|state| &state.generators)
                .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Tcp(_)))
                .count(),
            corpus.hosts
        );
        lowered.push(corpus.file);
    }
    assert_eq!(
        lowered,
        CAMPAIGN_CORPORA
            .iter()
            .map(|corpus| corpus.file)
            .collect::<Vec<_>>(),
        "every campaign corpus must have lowered"
    );
}

#[test]
#[ignore = "explicit T24 four-backend execution campaign"]
fn full_tcp_corpora_are_byte_identical_across_available_backends() {
    let corpora = selected_campaign_corpora();
    let mut compared = Vec::new();
    for corpus in corpora.iter().copied() {
        let mut backends = vec!["scalar"];
        let image = compile_config(path(corpus.file))
            .unwrap_or_else(|error| panic!("{} should lower: {error}", corpus.file));
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .unwrap_or_else(|error| panic!("{} Scalar failed: {error}", corpus.file));
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 4,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("{} CPU failed: {error}", corpus.file));
        assert_result_eq(&format!("{} CPU", corpus.file), &cpu.result, &scalar);
        backends.push("CPU");

        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        {
            let metal = run_metal_with_observations(
                &image,
                None,
                MetalConfig::default(),
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("{} Metal failed: {error}", corpus.file));
            assert_device_full_result_eq(&format!("{} Metal", corpus.file), &metal.result, &scalar);
            backends.push("Metal");
        }

        #[cfg(feature = "cuda")]
        {
            let cuda = run_cuda_with_observations(
                &image,
                None,
                CudaConfig::default(),
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("{} CUDA failed: {error}", corpus.file));
            assert_device_full_result_eq(&format!("{} CUDA", corpus.file), &cuda.result, &scalar);
            backends.push("CUDA");
        }

        // Positive evidence of executed work, so a campaign log can never be read as a green row
        // for a comparison that did not happen (T20g review round 1, M1).
        eprintln!(
            "T24 campaign: {} byte-identical across {} \
             (flows={} observed_packets={} departures={} arrivals={} pending_events={})",
            corpus.file,
            backends.join("/"),
            image.flows.len(),
            scalar.observed_packets.len(),
            scalar.departures.len(),
            scalar.arrivals.len(),
            scalar.pending_events.len(),
        );
        compared.push(corpus.file);
    }
    assert_eq!(
        compared,
        corpora.iter().map(|corpus| corpus.file).collect::<Vec<_>>(),
        "every selected corpus must have completed its cross-backend comparison"
    );
    assert!(
        !compared.is_empty(),
        "the campaign must compare at least one corpus"
    );
    eprintln!(
        "T24 campaign: compared {} corpora: {}",
        compared.len(),
        compared.join(", ")
    );
}
