# T24 TCP conformance corpora

These are pre-benchmark correctness corpora, not formal RQ9 ladder measurements. Each canonical
fat tree uses `k^3 / 4` hosts, `k / 2` hosts per edge switch, and one deterministic flow per host.
Reno and CUBIC have separate images so every algorithm is exercised at the full canonical scale.
The deliberately short 16-MSS transfers emphasize topology-scale closed-loop state and remain in
slow start; exact CUBIC congestion-avoidance arithmetic is exercised by the shared 160-MSS
four-backend adversarial matrix.

| Fixture | k | Hosts | Flows | Congestion control |
| --- | ---: | ---: | ---: | --- |
| `fattree_k16_tcp_reno_f1024.toml` | 16 | 1,024 | 1,024 | Reno |
| `fattree_k16_tcp_cubic_f1024.toml` | 16 | 1,024 | 1,024 | CUBIC |
| `fattree_k32_tcp_reno_f8192.toml` | 32 | 8,192 | 8,192 | Reno |
| `fattree_k32_tcp_cubic_f8192.toml` | 32 | 8,192 | 8,192 | CUBIC |

All fixtures use 100 Gbit/s links, 1 us propagation, 1,460-byte MSS, 23,360-byte transfers, a
1.152 ms horizon, exact loss-only TCP, and the T23 fixed CUBIC profile (`beta=0.7`, `c=0.4`, fast
convergence enabled). The `k4` fixture (`fattree_k4_tcp_cubic_f16_smoke.toml`) is only an RQ9
harness smoke and is deliberately not a ladder point, so it is absent from the table above. It is
still a canonical TCP image, so it is a full member of the lowering and four-backend byte-identity
campaigns in `validation/tests/t24_tcp_corpora.rs`, which run five corpora and reject a
`T24_CORPUS` filter that selects none of them.

Run a corpus through the sustained harness by passing its path as the first argument. The harness
retains four balanced-order samples and prints `workload=tcp rq=RQ9` on protocol, per-sample, and
summary records. Example:

```text
cargo run --release --features metal-spike --bin t15e_sustained_benchmark -- configs/benchmarks/tcp/fattree_k16_tcp_reno_f1024.toml
cargo run --release --features cuda --bin t15e_sustained_benchmark -- configs/benchmarks/tcp/fattree_k16_tcp_reno_f1024.toml
```
