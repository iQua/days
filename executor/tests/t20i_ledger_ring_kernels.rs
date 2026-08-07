//! T20i static gate: the two device ledger kernels must stay symmetric, and the pre-ring costs
//! must be deleted rather than gated.
//!
//! CUDA cannot be executed on this host, and the follow-up measurement round owns remote
//! execution, so the CUDA half of the ring conversion has no dynamic gate here. What *can* be
//! pinned without a GPU is that the CUDA kernel is a transliteration of the Metal kernel that the
//! Apple `tcp_semantics` suite does execute byte-identically. These tests extract the five ledger
//! functions from both sources, erase the language-level differences between Metal and CUDA, and
//! require the results to be identical character for character.

/// The ledger functions the ring conversion owns, in source order.
const LEDGER_FUNCTIONS: [&str; 5] = [
    "tcp_ledger_slot",
    "tcp_ledger_lower_bound",
    "tcp_ledger_find",
    "tcp_ledger_insert",
    "tcp_ledger_acknowledge",
];

const METAL: &str = include_str!("../src/metal_kernels.metal");
const CUDA: &str = include_str!("../src/cuda_kernels.cu");

/// Extracts one function definition, brace-balanced, from a kernel source.
fn extract(kernel: &str, declaration_prefixes: &[&str], name: &str) -> String {
    let mut lines = kernel.lines().peekable();
    while let Some(line) = lines.next() {
        let declares = declaration_prefixes
            .iter()
            .any(|prefix| line.starts_with(prefix))
            && (line.contains(&format!("{name}("))
                || line.trim_end().ends_with(&format!("{name}(")));
        if !declares {
            continue;
        }
        let mut body = vec![line.to_string()];
        let mut depth = line.matches('{').count() as isize - line.matches('}').count() as isize;
        let mut opened = line.contains('{');
        while !(opened && depth == 0) {
            let next = lines
                .next()
                .unwrap_or_else(|| panic!("{name} definition is unterminated"));
            depth += next.matches('{').count() as isize - next.matches('}').count() as isize;
            opened |= next.contains('{');
            body.push(next.to_string());
        }
        return body.join("\n");
    }
    panic!("{name} is not defined in this kernel source");
}

/// Erases the Metal/CUDA spelling differences so only semantics remain.
fn normalize(source: &str) -> String {
    let mut text = source.replace("__device__ __forceinline__ ", "");
    for (from, to) in [
        ("inline ulong ", "ulong "),
        ("inline bool ", "bool "),
        ("const device ulong *", "const ulong *"),
        ("device ulong *", "ulong *"),
        ("const thread ulong *", "const ulong *"),
        ("thread ulong &", "ulong &"),
        ("thread ulong *", "ulong *"),
    ] {
        text = text.replace(from, to);
    }
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn the_metal_and_cuda_ledger_kernels_are_transliterations_of_each_other() {
    for name in LEDGER_FUNCTIONS {
        let metal = normalize(&extract(METAL, &["inline ulong ", "inline bool "], name));
        let cuda = normalize(&extract(
            CUDA,
            &[
                "__device__ __forceinline__ ulong ",
                "__device__ __forceinline__ bool ",
            ],
            name,
        ));
        assert_eq!(
            metal, cuda,
            "{name} must be identical across the Metal and CUDA kernels once the language \
             spelling is erased; the Apple suite is the only backend that executes it locally"
        );
    }
}

#[test]
fn both_kernels_declare_the_six_word_ring_metadata_row() {
    assert!(METAL.contains("constant uint TCP_LEDGER_META_WORDS = 6;"));
    assert!(METAL.contains("constant uint TCP_LEDGER_META_HEAD = 4;"));
    assert!(METAL.contains("constant uint TCP_LEDGER_META_HIGH_WATER = 5;"));
    assert!(CUDA.contains("constexpr uint TCP_LEDGER_META_WORDS = 6;"));
    assert!(CUDA.contains("constexpr uint TCP_LEDGER_META_HEAD = 4;"));
    assert!(CUDA.contains("constexpr uint TCP_LEDGER_META_HIGH_WATER = 5;"));
}

#[test]
fn the_acknowledge_compaction_loop_is_deleted_not_gated() {
    // The pre-ring acknowledge copied every surviving record down to index 0. That loop is the
    // O(n) cost the ring exists to remove, so it must be absent from the source rather than
    // skipped at runtime.
    for (backend, kernel) in [("Metal", METAL), ("CUDA", CUDA)] {
        let acknowledge = extract(
            kernel,
            &["inline bool ", "__device__ __forceinline__ bool "],
            "tcp_ledger_acknowledge",
        );
        assert!(
            !acknowledge.contains("(keep + index)"),
            "{backend} acknowledge must not compact the surviving records"
        );
        assert!(
            acknowledge.contains("TCP_LEDGER_META_HEAD] = advanced"),
            "{backend} acknowledge must advance the ring head instead"
        );
        // Prefix removal walks only the records it removes; the sole surviving record access is
        // the partial-ACK boundary re-key, which reads the size and sequence and writes them back.
        assert_eq!(
            acknowledge.matches("tcp_state[first + ").count(),
            4,
            "{backend} acknowledge must touch only the partial-ACK boundary record"
        );
    }
}

#[test]
fn the_high_water_word_has_exactly_one_writer_in_each_kernel() {
    // Determinism of the fault payload rests on this: the high-water mark is a max over a
    // deterministic execution, raised only where `count` itself is raised.
    for (backend, kernel) in [("Metal", METAL), ("CUDA", CUDA)] {
        let writes = kernel
            .matches("tcp_state[meta + TCP_LEDGER_META_HIGH_WATER] =")
            .count();
        assert_eq!(
            writes, 1,
            "{backend} must raise the ledger high-water at exactly one site"
        );
        let insert = extract(
            kernel,
            &["inline bool ", "__device__ __forceinline__ bool "],
            "tcp_ledger_insert",
        );
        assert!(
            insert.contains("tcp_state[meta + TCP_LEDGER_META_HIGH_WATER] ="),
            "{backend}'s single high-water writer must be the insert path"
        );
    }
}

#[test]
fn neither_kernel_retains_a_linear_ledger_scan() {
    // Both the find scan and the insert-position scan became a binary search over the ring window.
    for (backend, kernel) in [("Metal", METAL), ("CUDA", CUDA)] {
        for name in ["tcp_ledger_find", "tcp_ledger_insert"] {
            let function = extract(
                kernel,
                &["inline bool ", "__device__ __forceinline__ bool "],
                name,
            );
            assert!(
                function.contains("tcp_ledger_lower_bound("),
                "{backend} {name} must locate a sequence by binary search"
            );
            assert!(
                !function.contains("for (ulong index = 0; index < count; ++index)"),
                "{backend} {name} must not walk the ledger linearly"
            );
        }
    }
}
