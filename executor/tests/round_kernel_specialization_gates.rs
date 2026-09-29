//! P14 spec: every Lane B entry point in both round kernels is guarded by the specialization
//! constant, so the plain `days_round` build compiles the DCQCN and PFC code out.
//!
//! The constant is the `MECHANISMS` template parameter in `cuda_kernels.cu` and the
//! `DAYS_MECHANISMS` function constant in `metal_kernels.metal`. A Lane B entry point is a call
//! into one of the functions Lane B added (listed below, from `git diff 948a0e9 b912af6`), or a
//! read of the PFC region offset or of a DCQCN receiver-row marker, made from code outside those
//! functions. Each must sit in a statement, or under an enclosing condition, that tests the
//! constant; an `else` branch inherits nothing from its `if`. The fingerprint suites prove the
//! mechanisms build is byte-identical; these gates prove the plain build cannot reach the code it
//! claims to compile out.

const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const METAL: &str = include_str!("../src/metal_kernels.metal");

/// The functions P14 Lane B added to both kernels.
const LANE_B_FUNCTIONS: [&str; 21] = [
    "dcqcn_apply_increase",
    "dcqcn_average_with_target",
    "dcqcn_checked_weighted_div",
    "dcqcn_cnp_arrival",
    "dcqcn_control_timer",
    "dcqcn_data_arrival",
    "dcqcn_on_bytes_emitted",
    "dcqcn_on_cnp",
    "dcqcn_on_control_timer",
    "dcqcn_pacing_timer",
    "emit_pfc_frame",
    "packet_incoming_link",
    "pfc_copy_queue_record",
    "pfc_enabled_ingress",
    "pfc_first_eligible",
    "pfc_flow_priority",
    "pfc_frame_arrival",
    "pfc_paused_mask",
    "pfc_priority_paused",
    "pfc_queue_row",
    "wfq_remove_at",
];

/// Plane reads that exist only for Lane B: the PFC region offset and the DCQCN receiver marker.
const LANE_B_READS: [&str; 2] = ["params[P_PFC_OFFSET]", "DCQCN_RECEIVER_NO_CNP"];

fn kernels() -> [(&'static str, &'static str, &'static str); 2] {
    [
        ("CUDA", CUDA, "MECHANISMS"),
        ("Metal", METAL, "DAYS_MECHANISMS"),
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
        let Some(opener) = (0..start).rev().find(|&at| {
            let line = code(lines[at]);
            !line.trim().is_empty() && indent(line) < depth && line.trim_end().ends_with('{')
        }) else {
            return false;
        };
        if indent(lines[opener]) == 0 {
            // The function header: nothing on the way up tested the constant.
            return false;
        }
        let header = statement_start(lines, opener);
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
        let calls = LANE_B_FUNCTIONS.iter().filter_map(|function| {
            let call = format!("{function}(");
            text.match_indices(&call)
                .any(|(at, _)| {
                    !text[..at]
                        .chars()
                        .next_back()
                        .is_some_and(|previous| previous.is_alphanumeric() || previous == '_')
                })
                .then(|| call.clone())
        });
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
        let sites = entry_points(source);
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
    let tokens = |source| {
        entry_points(source)
            .into_iter()
            .map(|(_, token)| token)
            .collect::<Vec<_>>()
    };
    let mut cuda = tokens(CUDA);
    let mut metal = tokens(METAL);
    cuda.sort_unstable();
    metal.sort_unstable();
    assert_eq!(
        cuda, metal,
        "the Metal port must guard exactly the CUDA entry points"
    );
}

#[test]
fn the_constant_selects_between_two_builds_of_one_round_body() {
    assert_eq!(
        CUDA.matches(
            "template <bool MECHANISMS>\n__device__ __forceinline__ void days_round_body("
        )
        .count(),
        1
    );
    for (build, value) in [("days_round", "false"), ("days_round_mechanisms", "true")] {
        assert!(
            CUDA.contains(&format!(
                "extern \"C\" __global__ __launch_bounds__(256) void {build}(DAYS_BUFFERS) {{\n    \
                 days_round_body<{value}>(DAYS_BUFFER_ARGS);\n}}"
            )),
            "{build}"
        );
    }
    assert!(METAL.contains("constant bool DAYS_MECHANISMS [[function_constant(0)]];"));
    assert_eq!(METAL.matches("[[function_constant(").count(), 1);
}

/// The plain build fails closed on every DCQCN or PFC source it can meet, with the one error the
/// hosts decode as `MechanismsKernelRequired`.
#[test]
fn the_plain_build_fails_closed_with_one_error_code() {
    for (backend, source, constant) in kernels() {
        let raise = "set_semantic_error(state, ERROR_MECHANISMS_REQUIRED";
        let raise_error = "set_semantic_error(error, ERROR_MECHANISMS_REQUIRED";
        let sites = source.matches(raise).count() + source.matches(raise_error).count();
        // The round-entry PFC check, the DCQCN generator timer, and the three packet kinds.
        assert_eq!(sites, 5, "{backend}");
        let lines = source.lines().collect::<Vec<_>>();
        for (index, line) in lines.iter().enumerate() {
            if line.contains(raise) || line.contains(raise_error) {
                assert!(
                    guarded(&lines, index, &format!("!{constant}")),
                    "{backend}:{}: the fail-closed stop must test !{constant}",
                    index + 1
                );
            }
        }
    }
}
