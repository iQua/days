use std::fs;

use days::scenario::compile_config;
use days_executor::{DropMarkPolicy, FlowGeneratorKind, GeneratorTermination, SchedulerKind};
use tempfile::TempDir;

fn compile_text(
    text: &str,
) -> Result<days_executor::SimulationImage, days::scenario::CompileError> {
    let directory = TempDir::new().expect("temporary scenario directory");
    let path = directory.path().join("scenario.toml");
    fs::write(&path, text).expect("write scenario");
    compile_config(path)
}

fn empty_config(duration: &str, port_rate: &str, capacity: u64, drop: &str, extra: &str) -> String {
    format!(
        r#"
seed = 26
duration = {duration}
edges = [[0, 2], [1, 2]]
hosts = [0, 1]

[switch]
port_rate = {port_rate}
capacity = {capacity}
discipline = "FIFO"
drop = "{drop}"
{extra}
"#,
    )
}

fn packet_distribution_config(
    duration: &str,
    initial_delay: &str,
    interval: &str,
    packet_size: &str,
    termination: &str,
) -> String {
    format!(
        r#"
seed = 26
duration = {duration}
edges = [[0, 2], [1, 2]]
hosts = [0, 1]

[switch]
port_rate = 18446744073709551615.0
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]

[flow.traffic]
initial_delay = {initial_delay}
{termination}
arr_dist = {{ type = "Uniform", low = {interval}, high = {interval} }}
pkt_size_dist = {{ type = "Uniform", low = {packet_size}, high = {packet_size} }}
"#,
    )
}

fn constant_generator(image: &days_executor::SimulationImage) -> days_executor::ConstantGenerator {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .find_map(|generator| match generator.kind {
            FlowGeneratorKind::Constant(constant) => Some(constant),
            _ => None,
        })
        .expect("constant generator")
}

#[test]
fn reviewer_numeric_fields_lower_exactly_above_binary64_integer_precision() {
    const EXACT: u64 = 9_007_199_254_740_993;
    let image = compile_text(&empty_config(
        "9007199.254740993",
        "9007199254740993.0",
        EXACT,
        "ECN_THRESHOLD",
        "ecn_threshold = 0.5",
    ))
    .expect("exact duration/rate/ECN scenario");
    assert_eq!(image.stop_time_ns, EXACT);
    assert!(image.links.iter().all(|link| link.rate_bps == EXACT));
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        let DropMarkPolicy::EcnThreshold(policy) = queue.drop_mark else {
            panic!("ECN threshold queue")
        };
        assert_eq!(policy.capacity, EXACT);
        assert_eq!(policy.threshold, 4_503_599_627_370_497);
    }

    let initial = compile_text(&packet_distribution_config(
        "10000000",
        "9007199.254740993",
        "0.000000001",
        "1.0",
        "duration = 0",
    ))
    .expect("exact initial delay");
    assert_eq!(constant_generator(&initial).first_departure_ns, EXACT);

    let interval = compile_text(&packet_distribution_config(
        "1",
        "0",
        "9007199.254740993",
        "1.0",
        "size = 1",
    ))
    .expect("exact arrival interval");
    assert_eq!(constant_generator(&interval).interval_ns, EXACT);

    let packet = compile_text(&packet_distribution_config(
        "1",
        "0",
        "0.000000001",
        "9007199254740993.0",
        "size = 9007199254740993",
    ))
    .expect("exact floating constant packet size");
    assert_eq!(constant_generator(&packet).packet_size_bytes, EXACT);
}

#[test]
fn decimal_scaled_classes_accept_u64_max_and_reject_nonintegral_or_one_past_values() {
    let image = compile_text(&empty_config(
        "18446744073.709551615",
        "18446744073709551615.0",
        100,
        "TailDrop",
        "",
    ))
    .expect("u64::MAX duration and rate");
    assert_eq!(image.stop_time_ns, u64::MAX);
    assert!(image.links.iter().all(|link| link.rate_bps == u64::MAX));

    let packet = compile_text(&packet_distribution_config(
        "18446744073.709551615",
        "0",
        "18446744073.709551615",
        "18446744073709551615.0",
        "duration = 0",
    ))
    .expect("u64::MAX interval and packet size");
    let constant = constant_generator(&packet);
    assert_eq!(constant.interval_ns, u64::MAX);
    assert_eq!(constant.packet_size_bytes, u64::MAX);
    assert_eq!(constant.termination, GeneratorTermination::DurationNs(0));

    for (literal, expected) in [
        (
            "9007199.2547409925",
            "exact representation requires an integer scaled value",
        ),
        ("18446744073.709551616", "exceeds the u64 representation"),
    ] {
        let error = compile_text(&empty_config(literal, "1", 100, "TailDrop", ""))
            .expect_err("invalid exact duration must reject")
            .to_string();
        assert!(error.contains(expected), "{literal}: {error}");
    }
    for literal in ["1.5", "18446744073709551616.0"] {
        let error = compile_text(&empty_config("1", literal, 100, "TailDrop", ""))
            .expect_err("invalid exact port rate must reject")
            .to_string();
        assert!(
            error.contains("integer scaled value") || error.contains("exceeds the u64"),
            "{literal}: {error}"
        );
    }
}

#[test]
fn cubic_literals_and_pfc_zero_timer_gates_are_exact() {
    let cubic = |beta: &str, c: &str| {
        format!(
            r#"
seed = 26
duration = 1
edges = [[0, 2], [1, 2]]
hosts = [0, 1]

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "TCP"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0
size = 1000
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}
[flow.traffic.tcp]
cc_algorithm = "Cubic"
[flow.traffic.tcp.cubic]
beta = {beta}
c = {c}
fast_convergence = true
"#,
        )
    };
    compile_text(&cubic("0.70", "4e-1")).expect("exactly equivalent CUBIC rationals");
    for (beta, c) in [
        ("0.70000000000000001", "0.4"),
        ("0.7", "0.40000000000000002"),
    ] {
        let error = compile_text(&cubic(beta, c))
            .expect_err("nearby binary64-equal CUBIC decimal must reject")
            .to_string();
        assert!(
            error.contains("unsupported TCP CUBIC parameters"),
            "{error}"
        );
    }

    let pfc = r#"
seed = 26
duration = 1
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"
[link]
mode = "Pfc"
[link.pfc]
xoff = [1, 0, 0, 0, 0, 0, 0, 0]
xon = [1, 0, 0, 0, 0, 0, 0, 0]
buffer_capacity = [2, 0, 0, 0, 0, 0, 0, 0]
pause_quanta = [1, 0, 0, 0, 0, 0, 0, 0]
refresh_interval = 1e-400
drain_interval = 0
"#;
    let error = compile_text(pfc)
        .expect_err("nonzero underflowed PFC timer must reject")
        .to_string();
    assert!(error.contains("PFC refresh/drain timers"), "{error}");
}

#[test]
fn integral_queue_red_wfq_and_link_fields_keep_their_exact_values() {
    const EXACT: u64 = 9_007_199_254_740_993;
    let config = format!(
        r#"
seed = 26
duration = 1
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
[switch]
port_rate = 8000000000
capacity = {EXACT}
discipline = "WFQ"
weights = [{EXACT}, 9223372036854775807]
drop = "TailDrop"
[link]
propagation_ns = {EXACT}
"#,
    );
    let image = compile_text(&config).expect("exact integral queue/link fields");
    assert!(image.links.iter().all(|link| link.propagation_ns == EXACT));
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        assert_eq!(queue.queue_capacity_packets, EXACT);
        let SchedulerKind::WeightedFairQueue(wfq) = &queue.scheduler else {
            panic!("WFQ queue")
        };
        assert_eq!(wfq.weights, vec![EXACT, 9_223_372_036_854_775_807]);
        assert_eq!(queue.drop_mark, DropMarkPolicy::TailDrop);
    }

    let red_capacity = 1_000_003_u64;
    let red = compile_text(&empty_config("1", "8000000000", red_capacity, "RED", ""))
        .expect("exact fixed RED thresholds");
    for queue in red.switch_states.iter().flat_map(|state| &state.queues) {
        let DropMarkPolicy::Red(red) = queue.drop_mark else {
            panic!("RED queue")
        };
        assert_eq!(red.capacity, red_capacity);
        assert_eq!(red.min_threshold, red_capacity * 7 / 10);
        assert_eq!(red.max_threshold, red_capacity * 9 / 10);
    }

    const TOML_INTEGER_MAX: u64 = i64::MAX as u64;
    let maximums = compile_text(&format!(
        r#"
seed = 26
duration = 1
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
[switch]
port_rate = 8000000000
capacity = {}
discipline = "WFQ"
weights = [{}]
drop = "ECN_THRESHOLD"
ecn_threshold = 1
[link]
propagation_ns = {}
"#,
        TOML_INTEGER_MAX, TOML_INTEGER_MAX, TOML_INTEGER_MAX,
    ))
    .expect("maximum TOML integer queue, ECN, WFQ, and link fields");
    assert!(
        maximums
            .links
            .iter()
            .all(|link| link.propagation_ns == TOML_INTEGER_MAX)
    );
    for queue in maximums
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
    {
        assert_eq!(queue.queue_capacity_packets, TOML_INTEGER_MAX);
        let SchedulerKind::WeightedFairQueue(wfq) = &queue.scheduler else {
            panic!("WFQ queue")
        };
        assert_eq!(wfq.weights, vec![TOML_INTEGER_MAX]);
        let DropMarkPolicy::EcnThreshold(ecn) = queue.drop_mark else {
            panic!("ECN threshold queue")
        };
        assert_eq!(ecn.capacity, TOML_INTEGER_MAX);
        assert_eq!(ecn.threshold, TOML_INTEGER_MAX);
    }
}
