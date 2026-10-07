//! P16 H1 fix round 1 (review F9): the stage-aware sizing's width rule charges each issue stream's
//! widest operation once at a host (ruling R11 (a)), with the streams the scenario declares
//! (`stream` on a `[[collective]]` or `[[compute]]`, or the workload IR's `Operation::stream`).
#![cfg(feature = "test")]

use days::scenario::compile_config;
use days_executor::SimulationImage;

fn compile_text(config: &str) -> SimulationImage {
    let file = tempfile::NamedTempFile::new().expect("temporary fixture must open");
    std::fs::write(file.path(), config).expect("temporary fixture must be written");
    compile_config(file.path()).unwrap_or_else(|error| panic!("fixture must lower: {error}"))
}

/// Review F9's two-stream host (ruling R11 (a)): 8 hosts on one switch; stream 0 runs `c0 -> a2a0
/// -> c1 -> c2 -> a2a1 -> c3` (RoCE all-to-alls, 7 roots per host) and a data-stream ReduceScatter
/// `w` (one ring root per host) follows `c1`.
fn two_streams(w_stream: Option<u32>, w_declared_mid: bool) -> String {
    let n = 8_u64;
    let hosts = (0..n).map(|h| h.to_string()).collect::<Vec<_>>().join(", ");
    let edges = (0..n)
        .map(|h| format!("[{h}, {n}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let sinks = (0..n)
        .map(|h| ((h + 1) % n).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let compute = |name: &str, after: &str| {
        format!(
            "\n[[compute]]\nname = \"{name}\"\nhosts = [{hosts}]\nduration_ns = 1000\n{after}\n"
        )
    };
    let collective = |name: &str, kind: &str, extra: &str| {
        format!(
            "\n[[collective]]\nname = \"{name}\"\ncollective_type = \"{kind}\"\nflow_type = \"RoCE\"\nflow_count = {n}\nsources = [{hosts}]\n{extra}\n[collective.traffic]\ninitial_delay = 0.0\nsize = {size}\narr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\npkt_size_dist = {{ type = \"DiscreteUniform\", low = 1000, high = 1000 }}\n\n[collective.traffic.dcqcn]\nrate_gbps = 1.0\nmax_rate_gbps = 1.0\npacing_interval_ns = 1000\n\n[collective.traffic.roce]\nretransmit_timeout_ns = 1000000\n",
            size = 20_000 * n
        )
    };
    let stream = w_stream.map_or_else(String::new, |stream| format!("stream = {stream}\n"));
    let w = collective(
        "w",
        "ReduceScatter",
        &format!("sinks = [{sinks}]\nafter = \"c1\"\n{stream}"),
    );
    let head = compute("c0", "")
        + &collective("a2a0", "AllToAll", "after = \"c0\"\n")
        + &compute("c1", "after = \"a2a0\"");
    let tail = compute("c2", "after = \"c1\"")
        + &collective("a2a1", "AllToAll", "after = \"c2\"\n")
        + &compute("c3", "after = \"a2a1\"");
    let body = if w_declared_mid {
        head + &w + &tail
    } else {
        head + &tail + &w
    };
    format!(
        "seed = 26\nedges = [{edges}]\nhosts = [{hosts}]\nduration = 0.05\n\n[switch]\nport_rate = 1000000000\ncapacity = 300\ndiscipline = \"FIFO\"\ndrop = \"TailDrop\"\n{body}"
    )
}

/// Review F9: with `w` on its own stream the width rule charges each stream's widest operation once,
/// 7 for stream 0's all-to-alls plus 1 for `w`, on every host and in either declaration order.
/// Untagged, `w` shares stream 0, and the chain cover may still split stream 0 (14): sound, but
/// not the ruled quantity, which needs the tag.
#[test]
fn a_data_stream_is_charged_once_beside_the_compute_stream() {
    for mid in [false, true] {
        let tagged = compile_text(&two_streams(Some(1), mid));
        assert_eq!(
            days_executor::stage_widths_for_testing(&tagged),
            vec![8; 8],
            "w on stream 1, declared {}",
            if mid { "between c1 and c2" } else { "last" }
        );
    }
    let untagged = compile_text(&two_streams(None, false));
    println!(
        "record=width untagged={:?}",
        days_executor::stage_widths_for_testing(&untagged)
    );
}

/// A flagship-shaped host program (MoE layers over a 16-rank expert group on one switch): per
/// microbatch and layer, the compute stream runs `forward -> dispatch -> expert -> combine` and,
/// backward, `backward -> dispatch -> expert -> combine -> weight gradient`; the data stream runs
/// one ReduceScatter per layer after that layer's weight gradient and its own previous
/// ReduceScatter (SimAI's data queue). `tagged` declares the data stream as stream 1.
fn flagship_shape(layers: usize, microbatches: usize, tagged: bool) -> String {
    let n = 16_u64;
    let hosts = (0..n).map(|h| h.to_string()).collect::<Vec<_>>().join(", ");
    let edges = (0..n)
        .map(|h| format!("[{h}, {n}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let sinks = (0..n)
        .map(|h| ((h + 1) % n).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let traffic = |size: u64| {
        format!(
            "\n[collective.traffic]\ninitial_delay = 0.0\nsize = {size}\narr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\npkt_size_dist = {{ type = \"DiscreteUniform\", low = 1000, high = 1000 }}\n\n[collective.traffic.dcqcn]\nrate_gbps = 100.0\nmax_rate_gbps = 100.0\npacing_interval_ns = 10\n\n[collective.traffic.roce]\nretransmit_timeout_ns = 0\nwindow_bytes = 20000\n"
        )
    };
    let mut body = String::new();
    let mut previous = String::new();
    let mut previous_rs = String::new();
    let mut op = 0;
    let mut name = |kind: &str| {
        op += 1;
        format!("{kind}{op}")
    };
    let after = |names: &[&str]| match names {
        [] => String::new(),
        [one] => format!("after = \"{one}\"\n"),
        many => format!(
            "after = [{}]\n",
            many.iter()
                .map(|name| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let compute = |body: &mut String, previous: &mut String, name: String| {
        let preds: Vec<&str> = if previous.is_empty() {
            vec![]
        } else {
            vec![previous.as_str()]
        };
        body.push_str(&format!(
            "\n[[compute]]\nname = \"{name}\"\nhosts = [{hosts}]\nduration_ns = 1000\n{}\n",
            after(&preds)
        ));
        *previous = name;
    };
    for _ in 0..microbatches {
        for pass in ["fwd", "bwd"] {
            for _ in 0..layers {
                compute(&mut body, &mut previous, name(pass));
                for phase in ["dispatch", "expert", "combine"] {
                    if phase == "expert" {
                        compute(&mut body, &mut previous, name("expert"));
                        continue;
                    }
                    let a2a = name(phase);
                    body.push_str(&format!(
                        "\n[[collective]]\nname = \"{a2a}\"\ncollective_type = \"AllToAll\"\nflow_type = \"RoCE\"\nflow_count = {n}\nsources = [{hosts}]\n{}{}",
                        after(&[previous.as_str()]),
                        traffic(16_000 * n)
                    ));
                    previous = a2a;
                }
                if pass == "bwd" {
                    compute(&mut body, &mut previous, name("wg"));
                    let rs = name("rs");
                    let preds: Vec<&str> = if previous_rs.is_empty() {
                        vec![previous.as_str()]
                    } else {
                        vec![previous.as_str(), previous_rs.as_str()]
                    };
                    body.push_str(&format!(
                        "\n[[collective]]\nname = \"{rs}\"\ncollective_type = \"ReduceScatter\"\nflow_type = \"RoCE\"\nflow_count = {n}\nsources = [{hosts}]\nsinks = [{sinks}]\n{}{}{}",
                        after(&preds),
                        if tagged { "stream = 1\n" } else { "" },
                        traffic(4_000 * n)
                    ));
                    previous_rs = rs;
                }
            }
        }
    }
    format!(
        "seed = 26\nedges = [{edges}]\nhosts = [{hosts}]\nduration = 0.05\n\n[switch]\nport_rate = 100000000000\ncapacity = 300\ndiscipline = \"FIFO\"\ndrop = \"TailDrop\"\n{body}"
    )
}

/// Review F9's flagship-shape effect: the plan of the same program with the data stream untagged
/// (the greedy cover before the fix) and tagged (each stream's widest operation once). Recorded,
/// and the tagged plan is never larger.
#[test]
fn the_flagship_shape_plans_with_its_streams() {
    let plane = |report: &days_executor::DeviceSizingReport, name: &str| {
        report
            .planes
            .iter()
            .find(|plane| plane.name == name)
            .map_or(0, |plane| plane.words)
    };
    for (layers, microbatches) in [(2, 2), (4, 2)] {
        let mut previous = None;
        for tagged in [false, true] {
            let image = compile_text(&flagship_shape(layers, microbatches, tagged));
            let widths = days_executor::stage_widths_for_testing(&image);
            let report = days_executor::size_default_device_plan(&image).expect("projects");
            println!(
                "record=flagship_shape layers={layers} microbatches={microbatches} tagged={tagged} width={} remote_staging={} host_queue={} total_device_bytes={}",
                widths[0],
                plane(&report, "remote_staging"),
                plane(&report, "host_queue_records"),
                report.total_device_bytes
            );
            if let Some(untagged) = previous {
                assert!(report.total_device_bytes <= untagged);
            }
            previous = Some(report.total_device_bytes);
        }
    }
}
