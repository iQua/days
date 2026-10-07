//! P14 spec: every Lane B entry point in both round kernels is guarded by the specialization
//! constant, so the plain `days_round` build compiles the DCQCN and PFC code out, and the plain
//! build carries no mechanism code of its own: no device-side stop (the host refuses the plain
//! kernel on mechanism plans, `device_mechanism::plain_round_kernel_refusal`) and no forwarding
//! function (round 2, `evidence/P14/zero-cost-diagnosis.md` §3.2).
//!
//! The constant is `MECHANISMS` in CUDA (a block-scope `constexpr bool` in each round kernel, ahead
//! of the included round body, and the template parameter of the helpers it calls) and the
//! `DAYS_MECHANISMS` function constant in Metal. A Lane B entry point is a call
//! into one of the functions Lane B added (listed below, from `git diff 948a0e9 b912af6`), or a
//! read of the PFC region offset or of a DCQCN receiver-row marker, made from code outside those
//! functions. Each must sit in a statement, or under an enclosing condition, that tests the
//! constant; an `else` branch inherits nothing from its `if`. The fingerprint suites prove the
//! mechanisms build is byte-identical; these gates prove the plain build cannot reach the code it
//! claims to compile out.

const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const METAL: &str = include_str!("../src/metal_kernels.metal");
/// The CUDA round body, included by both round kernels.
const CUDA_ROUND_BODY: &str = "src/cuda_round_body.inc";

/// The CUDA round body's text, read at run time so a missing file fails as a test.
fn cuda_round_body() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(CUDA_ROUND_BODY);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} must exist: {error}", path.display()))
}

/// The CUDA source as the compiler sees the round kernels: the kernel file and the round body.
fn cuda_source() -> String {
    format!("{CUDA}\n{}", cuda_round_body())
}

/// The functions P14 Lane B, P15 lane R4 (queue pairs, host-link PFC), P16 D1 (the Mellanox-form
/// DCQCN controller), P16 G1 (collective and compute stages) and P16 H4 (the parked bitsets)
/// added to both kernels.
const LANE_B_FUNCTIONS: [&str; 49] = [
    "compute_timer",
    "dcqcn_alpha_through",
    "dcqcn_cnp_arrival",
    "dcqcn_data_arrival",
    "dcqcn_decrease_check",
    "dcqcn_decrease_due",
    "dcqcn_due",
    "dcqcn_first_decrease_at_or_after",
    "dcqcn_increase_due",
    "dcqcn_increase_fire",
    "dcqcn_materialize",
    "dcqcn_materialize_if_due",
    "dcqcn_mul_shr63",
    "dcqcn_on_feedback",
    "dcqcn_pacing_timer",
    "dcqcn_settle",
    "emit_pfc_frame",
    "flow_route_mechanisms",
    "packet_egress_mechanisms",
    "packet_incoming_link",
    "packet_remote_target_mechanisms",
    "pfc_copy_queue_record",
    "pfc_enabled_ingress",
    "pfc_first_eligible",
    "pfc_flow_data_class",
    "pfc_frame_arrival",
    "pfc_mark_parked",
    "pfc_packet_priority",
    "pfc_parked_bitset",
    "pfc_paused_mask",
    "pfc_priority_paused",
    "pfc_queue_row",
    "roce_data_arrival",
    "roce_emit_timers",
    "roce_feedback_arrival",
    "roce_pacing_tick",
    "roce_packet_size",
    "roce_receive",
    "roce_receiver_packet",
    "roce_restart",
    "roce_resume_parked",
    "roce_settle",
    "roce_timeout",
    "roce_token_packet",
    "stage_after_event",
    "stage_prerequisites",
    "stage_release",
    "stage_unreleased",
    "wfq_remove_at",
];

/// Plane reads that exist only for Lane B: the PFC region offset, the DCQCN receiver marker and the
/// stage region offset (P16 G1).
const LANE_B_READS: [&str; 3] = [
    "params[P_PFC_OFFSET]",
    "DCQCN_RECEIVER_NO_CNP",
    "params[P_STAGE_OFFSET]",
];

fn kernels() -> [(&'static str, String, &'static str); 2] {
    [
        ("CUDA", cuda_source(), "MECHANISMS"),
        ("Metal", METAL.to_owned(), "DAYS_MECHANISMS"),
    ]
}

/// A source line without its trailing `//` comment.
fn code(line: &str) -> &str {
    line.find("//").map_or(line, |comment| &line[..comment])
}

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// The first line of the statement (or block header) that ends at line `index`.
fn statement_start(lines: &[&str], index: usize) -> usize {
    let mut start = index;
    while start > 0 {
        let previous = code(lines[start - 1]).trim();
        if previous.is_empty()
            || previous.ends_with(';')
            || previous.ends_with('{')
            || previous.ends_with('}')
        {
            break;
        }
        start -= 1;
    }
    start
}

/// The function a line belongs to: the name in the nearest column-0 header above it.
fn enclosing_function(lines: &[&str], index: usize) -> Option<String> {
    (0..=index).rev().find_map(|at| {
        let line = code(lines[at]);
        if indent(line) != 0 || line.trim().is_empty() || line.starts_with('}') {
            return None;
        }
        // The first `name(` on the header line, skipping `__launch_bounds__(...)`.
        let mut rest = line;
        while let Some(open) = rest.find('(') {
            let name = rest[..open]
                .rsplit(|character: char| !(character.is_alphanumeric() || character == '_'))
                .next()
                .unwrap_or_default();
            if !name.is_empty() && name != "__launch_bounds__" {
                return Some(name.to_owned());
            }
            rest = &rest[open + 1..];
        }
        None
    })
}

/// Whether line `index` sits in a statement, or under an enclosing condition, naming `constant`.
fn guarded(lines: &[&str], index: usize, constant: &str) -> bool {
    let mut start = statement_start(lines, index);
    if lines[start..=index]
        .iter()
        .any(|line| code(line).contains(constant))
    {
        return true;
    }
    loop {
        let depth = indent(lines[start]);
        // The enclosing block: the nearest header above whose first line is shallower. A
        // multi-line condition's continuation lines sit at the body's depth, so the header's depth
        // is its first line's.
        let Some(opener) = (0..start).rev().find(|&at| {
            code(lines[at]).trim_end().ends_with('{')
                && indent(lines[statement_start(lines, at)]) < depth
        }) else {
            return false;
        };
        let header = statement_start(lines, opener);
        if indent(lines[header]) == 0 {
            // The function header: nothing on the way up tested the constant.
            return false;
        }
        let text = lines[header..=opener]
            .iter()
            .map(|line| code(line))
            .collect::<Vec<_>>()
            .join("\n");
        if !text.contains("else") && text.contains(constant) {
            return true;
        }
        start = header;
    }
}

/// Every Lane B entry point outside the Lane B functions, as `(line number, token)`.
fn entry_points(source: &str) -> Vec<(usize, String)> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut sites = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let text = code(line);
        // A call is the name followed by `(`, or by template arguments (`name<MECHANISMS>(`).
        let calls = LANE_B_FUNCTIONS
            .iter()
            .filter(|function| {
                text.match_indices(**function).any(|(at, _)| {
                    let before = text[..at]
                        .chars()
                        .next_back()
                        .is_some_and(|previous| previous.is_alphanumeric() || previous == '_');
                    let after = text[at + function.len()..].chars().next();
                    !before && matches!(after, Some('(' | '<'))
                })
            })
            .map(|function| format!("{function}("));
        let reads = LANE_B_READS
            .iter()
            .filter(|read| text.contains(**read))
            .map(|read| (*read).to_owned());
        let tokens = calls.chain(reads).collect::<Vec<_>>();
        if tokens.is_empty() {
            continue;
        }
        let function = enclosing_function(&lines, index).unwrap_or_default();
        if LANE_B_FUNCTIONS.contains(&function.as_str()) || indent(text) == 0 {
            // A Lane B function's own body, or its definition line.
            continue;
        }
        for token in tokens {
            sites.push((index, token));
        }
    }
    sites
}

#[test]
fn every_lane_b_entry_point_tests_the_specialization_constant() {
    for (backend, source, constant) in kernels() {
        let lines = source.lines().collect::<Vec<_>>();
        let sites = entry_points(&source);
        assert!(
            sites.len() >= 20,
            "{backend}: the entry-point scan found only {} sites",
            sites.len()
        );
        let unguarded = sites
            .iter()
            .filter(|(index, _)| !guarded(&lines, *index, constant))
            .map(|(index, token)| format!("{}: {token} {}", index + 1, lines[*index].trim()))
            .collect::<Vec<_>>();
        assert!(
            unguarded.is_empty(),
            "{backend}: Lane B entry points outside the specialization guard:\n{}",
            unguarded.join("\n")
        );
    }
}

#[test]
fn both_kernels_guard_the_same_entry_points() {
    let tokens = |source: &str| {
        entry_points(source)
            .into_iter()
            .map(|(_, token)| token)
            .collect::<Vec<_>>()
    };
    let mut cuda = tokens(&cuda_source());
    let mut metal = tokens(METAL);
    cuda.sort_unstable();
    metal.sort_unstable();
    assert_eq!(
        cuda, metal,
        "the Metal port must guard exactly the CUDA entry points"
    );
}

/// One round body, included verbatim by both CUDA round kernels, each preceded by its own
/// `constexpr bool MECHANISMS`: no forwarding function. Compiled through a forwarding function, the
/// body lost the hoisting of launch-constant `params` loads, and neither kernel matched its
/// reference byte for byte (`evidence/P14/zero-cost-diagnosis.md` §1.4, §2.2).
#[test]
fn both_cuda_round_kernels_include_one_body_without_a_forwarding_function() {
    assert!(!CUDA.contains("days_round_body"), "no forwarding function");
    assert!(
        !CUDA.contains("DAYS_BUFFER_ARGS"),
        "no forwarded argument list"
    );
    // The mechanisms build states one resident block per SM: under `__launch_bounds__(256)` alone,
    // CUDA 13.0's ptxas targets 128 registers on sm_121 for it and spills (evidence/P14/spec.md
    // §4). The plain build keeps the allocation the probe measured.
    for (build, bounds, value) in [
        ("days_round", "256", "false"),
        ("days_round_mechanisms", "256, 1", "true"),
    ] {
        assert!(
            CUDA.contains(&format!(
                "extern \"C\" __global__ __launch_bounds__({bounds}) void {build}(DAYS_BUFFERS) {{\n    \
                 constexpr bool MECHANISMS = {value};\n#include \"cuda_round_body.inc\"\n}}"
            )),
            "{build}"
        );
    }
    assert_eq!(CUDA.matches("#include \"cuda_round_body.inc\"").count(), 2);
    let body = cuda_round_body();
    assert!(body.contains("dispatch_event<MECHANISMS>("));
    assert!(
        !body.contains("#include") && !body.contains("__global__"),
        "the body is statements only"
    );
    let build = include_str!("../build.rs");
    assert!(build.contains("cargo:rerun-if-changed=src/cuda_round_body.inc"));
    assert!(METAL.contains("constant bool DAYS_MECHANISMS [[function_constant(0)]];"));
    assert_eq!(METAL.matches("[[function_constant(").count(), 1);
}

/// P14 round 3: each round kernel is emitted by its own compile of the kernel file, so the plain
/// and mechanisms modules each carry every shared kernel and exactly one round kernel
/// (`evidence/P14/modprobe-ab.md`: a module holding both moved the code placed after them).
#[test]
fn each_round_kernel_is_built_into_its_own_module() {
    assert!(CUDA.contains(
        "#if !defined(DAYS_ROUND_MODULE) || (DAYS_ROUND_MODULE != 0 && DAYS_ROUND_MODULE != 1)\n#error"
    ));
    let plain = CUDA
        .find("#if DAYS_ROUND_MODULE == 0\n")
        .expect("the plain module's branch");
    let split = CUDA[plain..]
        .find("#else\n")
        .expect("the mechanisms branch")
        + plain;
    let end = CUDA[split..]
        .find("#endif\n")
        .expect("the end of the branches")
        + split;
    assert!(CUDA[plain..split].contains("void days_round(DAYS_BUFFERS)"));
    assert!(!CUDA[plain..split].contains("days_round_mechanisms("));
    assert!(CUDA[split..end].contains("void days_round_mechanisms(DAYS_BUFFERS)"));
    assert!(!CUDA[split..end].contains("void days_round("));
    assert_eq!(CUDA.matches("void days_round(DAYS_BUFFERS)").count(), 1);
    assert_eq!(
        CUDA.matches("void days_round_mechanisms(DAYS_BUFFERS)")
            .count(),
        1
    );

    let build = include_str!("../build.rs");
    for (module, fatbin) in [
        ("0", "days_cuda_kernels.fatbin"),
        ("1", "days_cuda_kernels_mechanisms.fatbin"),
    ] {
        assert!(build.contains(&format!("(\"{module}\", out_dir.join(\"{fatbin}\"))")));
    }
    assert!(build.contains("-DDAYS_ROUND_MODULE={module}"));
    let cuda_host = include_str!("../src/cuda.rs");
    for fatbin in [
        "\"/days_cuda_kernels.fatbin\"",
        "\"/days_cuda_kernels_mechanisms.fatbin\"",
    ] {
        assert_eq!(
            cuda_host.matches(fatbin).count(),
            1,
            "{fatbin} is loaded once"
        );
    }
}

/// The plain build carries no mechanism code: every Lane B entry point is guarded by the constant
/// (above), and nothing tests its negation. There is no device-side stop and no mechanism error
/// code; the host check of the uploaded plan is the plain kernel's only fail-closed path.
#[test]
fn the_plain_build_has_no_device_side_stop() {
    for (backend, source, constant) in kernels() {
        let negated = format!("!{constant}");
        let lines = source
            .lines()
            .enumerate()
            .filter(|(_, line)| code(line).contains(&negated))
            .map(|(index, line)| format!("{}: {}", index + 1, line.trim()))
            .collect::<Vec<_>>();
        assert!(lines.is_empty(), "{backend}: {}", lines.join("\n"));
        assert!(
            !source.contains("ERROR_MECHANISMS_REQUIRED"),
            "{backend}: no mechanism error code"
        );
    }
}
