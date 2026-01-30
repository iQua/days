# Experiment results

## Benchmarks (LeanGuard vs TLC)

- Timestamp: `2026-01-30T16:21:24`
- Raw data: `logs/bench_leanguard_vs_tlc_2026-01-30.json`
- CSV (runs): `logs/bench_leanguard_vs_tlc_2026-01-30_runs.csv`
- CSV (summary): `logs/bench_leanguard_vs_tlc_2026-01-30_summary.csv`

| Protocol | Config | Events | checker_ms | tlc_total_ms | tlc_cmd_ms | tlc_total/checker | TLC statuses |
|---|---|---:|---:|---:|---:|---:|---|
| aqm | configs/cubic_simple.toml | 12 | 9.8 ± 1.9 | 601.4 ± 16.4 | 597.8 ± 16.9 | 63.7 | accept |
| aqm | configs/dcqcn_10s.toml | 242 | 22.0 ± 9.6 | 663.2 ± 8.2 | 650.0 ± 7.8 | 46.5 | accept |
| aqm | configs/dcqcn_1s.toml | 242 | 6.6 ± 1.3 | 689.6 ± 22.6 | 676.8 ± 22.8 | 108.0 | accept |
| aqm | configs/dcqcn_2s.toml | 242 | 5.8 ± 1.3 | 664.8 ± 12.6 | 651.8 ± 12.6 | 118.6 | accept |
| aqm | configs/dcqcn_multi.toml | 1151 | 10.0 ± 2.1 | 871.6 ± 39.9 | 815.0 ± 39.2 | 89.8 | accept |
| aqm | configs/dcqcn_simple.toml | 242 | 14.2 ± 10.5 | 685.8 ± 72.9 | 670.2 ± 68.6 | 60.5 | accept |
| aqm | configs/drr_simple.toml | 400 | 9.2 ± 2.8 | 701.0 ± 18.5 | 678.6 ± 19.6 | 84.5 | accept |
| aqm | configs/pfc.toml | 4002 | 28.2 ± 8.6 | 1292.6 ± 10.5 | 1112.4 ± 11.0 | 49.5 | accept |
| aqm | configs/wfq_simple.toml | 400 | 9.2 ± 3.1 | 699.2 ± 11.8 | 676.8 ± 9.7 | 83.3 | accept |
| cubic | configs/cubic_simple.toml | 6 | 12.2 ± 8.9 | 694.8 ± 26.2 | 688.4 ± 25.2 | 73.2 | accept |
| dcqcn | configs/dcqcn_10s.toml | 100084 | 598.4 ± 15.3 | 25082.6 ± 1285.8 | 20251.0 ± 1304.6 | 42.0 | accept |
| dcqcn | configs/dcqcn_1s.toml | 10084 | 67.2 ± 9.0 | 2814.4 ± 132.1 | 2288.2 ± 121.6 | 42.4 | accept |
| dcqcn | configs/dcqcn_2s.toml | 20084 | 117.2 ± 2.4 | 4594.4 ± 76.1 | 3600.2 ± 79.4 | 39.2 | accept |
| dcqcn | configs/dcqcn_multi.toml | 4298 | 32.4 ± 3.4 | 1880.8 ± 66.8 | 1641.8 ± 62.7 | 58.5 | accept |
| dcqcn | configs/dcqcn_simple.toml | 2084 | 26.6 ± 10.2 | 1273.0 ± 36.1 | 1137.2 ± 37.0 | 52.3 | accept |
| drr | configs/drr_simple.toml | 800 | 14.8 ± 12.1 | 4502.6 ± 63.2 | 4442.0 ± 63.4 | 437.7 | accept |
| pfc | configs/pfc.toml | 104 | 14.2 ± 7.9 | 639.4 ± 10.2 | 626.8 ± 10.8 | 53.7 | accept |
| wfq | configs/wfq_simple.toml | 1200 | 16.8 ± 12.6 | 1594.2 ± 16.4 | 1526.2 ± 9.7 | 127.2 | accept |

## Fault-injection agreement (LeanGuard vs TLC)

- Timestamp: `2026-01-30T16:15:57`
- Raw data: `logs/fault_injection_agreement_2026-01-30.json`
- CSV: `logs/fault_injection_agreement_2026-01-30.csv`

| Protocol | Case | Lean | TLC | Lean first failure | TLC first failure |
|---|---|---|---|---|---|
| aqm | accept_smoke | accept | accept |  |  |
| aqm | invalid_ecn_mark | reject | reject | line 2 (0,0,decision) | idx 1 (0,0,decision) |
| pfc | accept_smoke | accept | accept |  |  |
| pfc | pfc_recv_sender_mismatch | reject | reject | line 4 (256000000,2,pfc_recv) | idx 3 (256000000,2,pfc_recv) |
| dcqcn | accept_smoke | accept | accept |  |  |
| dcqcn | alpha_mismatch_first_row | reject | reject | line 2 (100000,0,timer_tick) | idx 1 (100000,0,timer_tick) |
| dcqcn | cnp_size_mismatch | reject | reject | line 11 (952000,9,cnp_sent) | idx 10 (952000,9,cnp_sent) |
| wfq | accept_smoke | accept | accept |  |  |
| wfq | finish_time_mismatch | reject | reject | line 3 (0,1,schedule) | idx 2 (0,1,schedule) |
| drr | accept_smoke | accept | accept |  |  |
| drr | deficit_mismatch | reject | reject | line 3 (0,1,schedule) | idx 2 (0,1,schedule) |
| cubic | accept_smoke | accept | accept |  |  |
| cubic | cwnd_mismatch | reject | reject | line 2 (1000000000,0,timeout) | idx 1 (1000000000,0,timeout) |

## Notes / caveats

- The TLC baseline for `CubicTrace.tla` is validated against `configs/cubic_simple.toml`.
- `configs/benchmarks/leanguard/tcp_cubic_micro.toml` currently produces a TLC **reject** for `CubicTrace.tla` (the fixed-point approximation drifts on longer traces), so it should not be used as an “accept” benchmark without adjusting either the config or the spec.
