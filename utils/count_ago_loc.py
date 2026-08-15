"""Count Days AGO lines of code, Rust and C++ separately, excluding tests.

Methodology mirrors utils/count_loc.py (the counter used for the legacy Days
paper): a line counts when it is non-empty and does not start with a line
comment; in Rust files, counting stops at the first `#[cfg(test)]` marker;
`tests/` and `target/` directories are skipped entirely.

Scope (Days AGO, not legacy):
  Rust : src/ (shared scenario/topology front end), executor/, validation/
  C++  : CUDA (.cu/.cuh) and Metal (.metal) kernels plus any .cpp/.cc/.h
         under the same roots
Excluded: legacy/ (the legacy engine), xtask/ (build tooling),
crates/ (vendored third-party nexosim), hidden directories (.claude
worktrees), tests/ and target/ anywhere.
"""

import os
import re

RUST_ROOTS = ["src", "executor", "validation"]
CPP_EXTS = (".cu", ".cuh", ".metal", ".cpp", ".cc", ".h")
SKIP_PARTS = {"tests", "target"}


def countable_lines(file_path, rust):
    n = 0
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if rust and re.match(r"^#\[cfg\(test\)\]", line):
                break
            if not line or line.startswith("//"):
                continue
            n += 1
    return n


def walk(root):
    for dirpath, dirnames, filenames in os.walk(root):
        parts = dirpath.split(os.sep)
        if any(p in SKIP_PARTS for p in parts) or any(
            p.startswith(".") and p not in (".", "..") for p in parts
        ):
            dirnames[:] = []
            continue
        for name in filenames:
            yield os.path.join(dirpath, name)


def main():
    rust_total, cpp_total = 0, 0
    per_root = {}
    for root in RUST_ROOTS:
        rust_here, cpp_here = 0, 0
        for path in walk(root):
            if path.endswith(".rs"):
                rust_here += countable_lines(path, rust=True)
            elif path.endswith(CPP_EXTS):
                cpp_here += countable_lines(path, rust=False)
        per_root[root] = (rust_here, cpp_here)
        rust_total += rust_here
        cpp_total += cpp_here

    for root, (r, c) in per_root.items():
        print(f"{root:12s} Rust {r:7d}   C++ {c:7d}")
    print(f"{'TOTAL':12s} Rust {rust_total:7d}   C++ {cpp_total:7d}")


if __name__ == "__main__":
    main()
