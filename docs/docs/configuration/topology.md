# Topology Configuration

Topology parsing and graph construction is implemented in `src/topos/build.rs`.

Days supports three input modes:

1. **Fat-tree** (`[topology] category = "FatTree"`)
2. **Torus** (`[topology] category = "Torus"`)
3. **Custom graph** (omit `[topology]`, provide `edges` and `hosts`)

All modes ultimately produce:

- an undirected graph `UnGraph<usize, ()>` (nodes are `usize` IDs),
- a `hosts: Vec<usize>` list indicating which nodes can host flow endpoints.

## 1) Custom topology (explicit edges)

Use this mode when you want exact control over the graph:

```toml
edges = [[0, 1], [0, 2]]
hosts = [0, 1, 2]
```

- `edges` is a list of undirected edges (pairs of node IDs).
- `hosts` is a list of node IDs that can host sources/sinks.

## 2) Fat-tree

```toml
[topology]
category = "FatTree"

[topology.fat_tree]
k = 8
```

Constraints:

- `k` must be even and positive.

Notes:

- Days constructs the canonical k-ary fat-tree and sets `hosts = 0..(k^2/2)`.

## 3) Torus

```toml
[topology]
category = "Torus"

[topology.torus]
dim = 2
n = 3
```

Constraints:

- `dim` must be 1, 2, or 3.
- `n` must be positive.

Notes:

- Days builds a wrap-around torus and sets `hosts = 0..(n^dim)`.

## Common failure modes

- Missing the required sub-table (`[topology.fat_tree]` or `[topology.torus]`) for the chosen category.
- Using fewer than 2 hosts when you rely on random endpoint selection (flow sets, collective endpoint generation).
