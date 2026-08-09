//! Kernel-span extraction shared by the two T21 source gates.
//!
//! **Why this exists as a shared module.** Both `t21_fel_root_cache.rs` and
//! `t21_control_regrid.rs` assert over the *body of one kernel*, and both originally carried their
//! own copy of a helper that split the source on the literal opener
//! `extern "C" __global__ void `. That literal does not match
//!
//! ```text
//! extern "C" __global__ __launch_bounds__(1024) void days_round(DAYS_BUFFERS) {
//! ```
//!
//! which is the one CUDA entry point in the tree carrying an attribute between `__global__` and
//! `void`. Because the helper also *terminated* each body at the next occurrence of that same
//! literal, `days_round_prepare`'s extracted body silently ran on through the whole of
//! `days_round` — 170 lines instead of 66. The adversarial review demonstrated both consequences
//! with probes: the fix-2 recompute count was being measured over `days_round_prepare ∪
//! days_round`, and the "retained 1,024-lane width" assertion could not fail, because
//! `__launch_bounds__(1024)` on `days_round`'s signature satisfied `body.contains("1024")` even
//! after every real `1024` in `days_round_prepare` was rewritten to `512`.
//!
//! The extraction below is form-agnostic: it enumerates every entry point in a source, whatever
//! attributes its declaration carries, and bounds each body at the **next entry point of any
//! form**. One copy, used by both gates, so the two cannot drift apart again.

// This module is compiled separately into each gate binary, and neither uses every item, so an
// item unused by one of them is not dead code.
#![allow(dead_code)]

/// The token that opens a CUDA entry-point declaration, before any attributes.
pub const CUDA_MARKER: &str = "extern \"C\" __global__";
/// The token that opens a Metal entry-point declaration.
pub const METAL_MARKER: &str = "kernel";

/// One entry point: its name, the byte offset its declaration starts at, and the byte offset just
/// past the `(` that opens its argument list.
struct Entry {
    name: String,
    declaration: usize,
    body: usize,
}

/// Every entry point in `source`, in declaration order.
///
/// A declaration is a line that *starts* with `marker` — which is what keeps mentions of `kernel`
/// or `__global__` inside comments from being mistaken for one — followed, on that same line, by
/// optional attributes, then `void`, then the entry name, then `(`.
fn entries(source: &str, marker: &str) -> Vec<Entry> {
    let mut found = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find(marker) {
        let declaration = cursor + offset;
        cursor = declaration + marker.len();
        if declaration != 0 && !source[..declaration].ends_with('\n') {
            continue;
        }
        let rest = &source[cursor..];
        let line = &rest[..rest.find('\n').unwrap_or(rest.len())];
        let Some(void_at) = line.find("void ") else {
            continue;
        };
        let after_void = &line[void_at + "void ".len()..];
        let Some(paren) = after_void.find('(') else {
            continue;
        };
        let name = after_void[..paren].trim();
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        found.push(Entry {
            name: name.to_string(),
            declaration,
            body: cursor + void_at + "void ".len() + paren + 1,
        });
    }
    found
}

/// Trims the run of blank and `//` lines at the end of a span: they are the *next* kernel's header
/// comment, not part of this kernel.
fn without_the_next_kernels_header(body: &str) -> &str {
    let mut end = body.len();
    while end > 0 {
        let start = body[..end].rfind('\n').map_or(0, |index| index + 1);
        let line = body[start..end].trim();
        if line.is_empty() || line.starts_with("//") {
            end = start.saturating_sub(1);
        } else {
            break;
        }
    }
    &body[..end]
}

/// The body of `entry`, from just past its opening `(` to the start of the next entry point's
/// declaration, whatever form that declaration takes.
///
/// Panics if `entry` is not declared in `source` — a gate that names a kernel which no longer
/// exists must fail loudly rather than assert over an empty span.
pub fn kernel_body<'a>(source: &'a str, marker: &str, entry: &str) -> &'a str {
    let found = entries(source, marker);
    let index = found
        .iter()
        .position(|candidate| candidate.name == entry)
        .unwrap_or_else(|| panic!("`{entry}` must exist in the kernel source"));
    let end = found
        .get(index + 1)
        .map_or(source.len(), |next| next.declaration);
    without_the_next_kernels_header(&source[found[index].body..end])
}

/// The names of every entry point in `source`, in declaration order. Used by the gates to prove
/// the extraction sees the kernels they think it sees.
pub fn kernel_names(source: &str, marker: &str) -> Vec<String> {
    entries(source, marker)
        .into_iter()
        .map(|entry| entry.name)
        .collect()
}
