//! P16 H2: lowering on the Spectrum-X rail fabric (per-class link rates and delays, refusals).

use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{NodeKind, SimulationImage};
use tempfile::TempDir;

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn mini_rail() -> SimulationImage {
    compile_config(repo_path("configs/p16/rail_mini_roce.toml"))
        .unwrap_or_else(|error| panic!("configs/p16/rail_mini_roce.toml must lower: {error}"))
}

fn lower_variant(edit: impl FnOnce(String) -> String) -> Result<SimulationImage, String> {
    let text = fs::read_to_string(repo_path("configs/p16/rail_mini_roce.toml")).expect("fixture");
    let directory = TempDir::new().expect("temp dir");
    let path = directory.path().join("variant.toml");
    fs::write(&path, edit(text)).expect("write variant");
    compile_config(&path).map_err(|error| error.to_string())
}

#[test]
fn rail_links_take_their_class_rate_and_delay() {
    let image = mini_rail();
    let kind = |node: days_executor::NodeId| image.nodes[node.0 as usize].kind;
    let mut host_links = 0;
    let mut uplinks = 0;
    let mut lanes = 0;
    for link in &image.links {
        if kind(link.source) == NodeKind::Host && kind(link.target) == NodeKind::Host {
            // A same-server notify lane: two NVLink hops (tests/p16_stage_notify.rs).
            assert_eq!(
                (link.rate_bps, link.propagation_ns),
                (2_400_000_000_000, 50)
            );
            lanes += 1;
            continue;
        }
        assert_eq!(link.propagation_ns, 500, "link {:?}", link.id);
        if kind(link.source) == NodeKind::Host || kind(link.target) == NodeKind::Host {
            assert_eq!(link.rate_bps, 100_000_000_000, "NIC link {:?}", link.id);
            host_links += 1;
        } else {
            assert_eq!(link.rate_bps, 400_000_000_000, "uplink {:?}", link.id);
            uplinks += 1;
        }
    }
    // 8 GPUs x 2 directions; 4 ASWs x 2 PSWs x 2 directions; the ring's 4 same-server hops.
    assert_eq!((host_links, uplinks, lanes), (16, 16, 4));
    assert_eq!(image.host_states.len(), 8);
}

#[test]
fn rail_scenarios_refuse_the_single_rate_and_delay_keys() {
    let rated = lower_variant(|text| text.replace("[switch]\n", "[switch]\nport_rate = 1000\n"))
        .expect_err("a port rate on the rail fabric is refused");
    assert!(rated.contains("switch.port_rate"), "{rated}");
    let delayed = lower_variant(|text| {
        text.replace(
            "[link]\nmode = \"Pfc\"\n",
            "[link]\nmode = \"Pfc\"\npropagation_ns = 9\n",
        )
    })
    .expect_err("a uniform delay on the rail fabric is refused");
    assert!(delayed.contains("propagation"), "{delayed}");
}

#[test]
fn other_topologies_still_require_a_port_rate() {
    let directory = TempDir::new().expect("temp dir");
    let path = directory.path().join("unrated.toml");
    let text = fs::read_to_string(repo_path("configs/p15/roce_ring_allreduce_lossless.toml"))
        .expect("fixture")
        .replace("port_rate = 1000000000\n", "");
    fs::write(&path, text).expect("write");
    let error = compile_config(&path).expect_err("no rate").to_string();
    assert!(error.contains("`switch.port_rate` is missing"), "{error}");
}
