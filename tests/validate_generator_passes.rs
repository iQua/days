//! P16 H4: validation passes over each host's generator table a fixed number of times, however
//! many queue-pair receivers the image holds.
//!
//! `validate_generators` checked each DCQCN and RoCE receiver's generator kind with a scan over
//! every host's generators, so validation cost grew with receivers times generators: on P16 G2's
//! RESUME-scan gate (5 hosts, 8k stage queue pairs per host) it was most of the validation
//! instructions, and at the flagship's queue-pair counts it would not finish. The counter
//! (`planner-test-hooks`) counts calls of the per-host generator iterator.
//!
//! P16 H3 part 2B: the same holds for the checks that look up one flow's generator at its source
//! host (each RoCE pacing-timer token, each CNP, feedback and RoCE packet, each flow whose
//! feedback class differs from its data class). A scan of the host's table per lookup cost
//! queue pairs squared per host: at the flagship (about 14,000 queue pairs per host) validation
//! did not finish the lowering in 8 minutes.
#![cfg(feature = "test")]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{Backend, SimulationImage, take_generator_passes_for_testing, validate};

/// P16 G2's RESUME-scan gate fabric with `k` RoCE RingAllReduce collectives over five hosts: `8k`
/// stage queue pairs per host, each with a receiver at its target.
fn ring_image(k: usize) -> SimulationImage {
    let mut text = String::from(
        "seed = 42\nedges = [[0, 1], [1, 2], [2, 3], [3, 4]]\nhosts = [0, 1, 2, 3, 4]\n\
         duration = 2.0\n\n[switch]\nport_rate = 1000000000\ncapacity = 300\n\
         discipline = \"FIFO\"\ndrop = \"ECN_THRESHOLD\"\necn_threshold = 1.0\n\n\
         [link]\nmode = \"Pfc\"\n\n[link.pfc]\nhost_links = true\n\
         buffer_capacity = [0, 0, 0, 100000, 0, 0, 0, 0]\nxoff = [0, 0, 0, 20000, 0, 0, 0, 0]\n\
         xon = [0, 0, 0, 10000, 0, 0, 0, 0]\n",
    );
    for index in 0..k {
        text.push_str(&format!(
            "\n[[collective]]\nname = \"allreduce{index}\"\ncollective_type = \"RingAllReduce\"\n\
             flow_type = \"RoCE\"\npriority = 3\nflow_count = 5\nsources = [0, 2, 4, 1, 3]\n\
             sinks = [2, 4, 1, 3, 0]\n\n[collective.traffic]\ninitial_delay = 0.0\nsize = 100000\n\
             arr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\n\
             pkt_size_dist = {{ type = \"DiscreteUniform\", low = 1000, high = 1000 }}\n\n\
             [collective.traffic.dcqcn]\nrate_gbps = 1.0\nmin_rate_gbps = 0.01\n\
             max_rate_gbps = 1.0\ng = 0.00390625\nai_rate_gbps = 0.005\nhai_rate_gbps = 0.05\n\
             rp_timer_ns = 50000\npacing_interval_ns = 1000\n\n\
             [collective.traffic.roce]\nretransmit_timeout_ns = 0\nfeedback_priority = 0\n"
        ));
    }
    let directory = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("validate_generator_passes");
    std::fs::create_dir_all(&directory).expect("scratch directory");
    let path = directory.join(format!("ring_k{k}.toml"));
    let partial = directory.join(format!(
        "ring_k{k}.{:?}.partial",
        std::thread::current().id()
    ));
    std::fs::write(&partial, text).expect("write the image");
    std::fs::rename(&partial, &path).expect("publish the image");
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn roce_receivers(image: &SimulationImage) -> usize {
    image
        .host_states
        .iter()
        .filter_map(|host| host.roce_receivers.as_deref())
        .map(<[_]>::len)
        .sum()
}

#[test]
fn validation_passes_over_each_host_generator_table_a_fixed_number_of_times() {
    let mut passes = Vec::new();
    for k in [2, 8] {
        let image = ring_image(k);
        assert_eq!(roce_receivers(&image), 40 * k, "8k receivers per host");
        take_generator_passes_for_testing();
        validate(&image, Backend::Metal).expect("the image validates");
        passes.push((
            k,
            image.host_states.len(),
            take_generator_passes_for_testing(),
        ));
    }
    eprintln!("record=validate_generator_passes {passes:?}");
    assert!(
        passes
            .iter()
            .all(|(_, hosts, count)| *count == PASSES_PER_HOST * hosts),
        "{PASSES_PER_HOST} passes per host, independent of the receivers: {passes:?}"
    );
}

/// Validation's passes over one host's generator table. It was 53 until P16 H3 part 2B, when the
/// PFC headroom check began bounding every controlled link's frames in one pass instead of one
/// pass per PFC ingress monitor (19), and validate_flows began checking each host's table order
/// once (20).
const PASSES_PER_HOST: usize = 20;
