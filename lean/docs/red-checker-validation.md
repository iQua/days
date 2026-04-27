# RED checker validation

Validation date: 2026-04-27.

This note records the end-to-end validation run for the corrected RED checker.

## LeanGuard build and fixtures

Commands run from `/Users/bli/Playground/days`:

```bash
cd lean && lake build aqm_check
bash lean/scripts/run-aqm-fixtures.sh
bash lean/scripts/run-aqm-coverage-fixtures.sh
cargo test --features test -- --show-output
```

Results:

- `lake build aqm_check`: passed.
- AQM semantic fixture suite: passed.
- AQM coverage fixture suite: passed.
- Rust test suite: passed, including `157` library tests and all integration
  tests.

## Representative Days traces

Checked existing AQM logs with
`/Users/bli/Playground/days/lean/.lake/build/bin/aqm_check`.

- `leanguard_corpus/accepted/*/logs/aqm_events.csv`: all accepted. These traces
  use `ecn_threshold`.
- `logs/local_time/aqm_events.csv`: accepted. This is the representative RED
  trace in the checked-in logs and contains `382535` RED rows.
- `logs/drr_test/aqm_events.csv` and `logs/sp_test/aqm_events.csv`: accepted.
  These traces use `tail_drop`.
- `logs/drop_test_red_early_drop/aqm_events.csv`,
  `logs/drop_test_taildrop_small_buffer/aqm_events.csv`, and
  `logs/port_test/aqm_events.csv`: rejected as `empty CSV`. These are empty
  stale log files, not RED semantic incompatibilities.

No checked-in representative Days RED/RED_ECN trace failed under the corrected
checker.

## ns-3.47 experiment

The ns-3.47 checkout at `/Users/bli/Playground/ns-3.47` was fully built:

```bash
cd /Users/bli/Playground/ns-3.47
./ns3 build
```

Result: passed.

### Existing scratch source as copied

The existing source in
`/Users/bli/Playground/lean-paper/ns-experiments/leanguard-red-existing-bug.cc`
was copied into ns-3.47 `scratch/` and run:

```bash
cd /Users/bli/Playground/ns-3.47
./ns3 build leanguard-red-existing-bug
./ns3 run 'leanguard-red-existing-bug --out=/tmp/ns3-347-leanguard-red-existing-bug.csv' --no-build
/Users/bli/Playground/days/lean/.lake/build/bin/aqm_check /tmp/ns3-347-leanguard-red-existing-bug.csv
```

Result:

```text
REJECT: line 2: missing RED witness
```

The copied scratch source uses `MinTh = MaxTh = 2.0`, which produces
`red_min_threshold_ppb == red_max_threshold_ppb`. The corrected checker rejects
that malformed RED configuration before reaching the intended full-queue
`mark_ecn` row.

### Retuned ns-3.47 run

A temporary ignored ns-3.47 scratch source was retuned to preserve the same
full-queue behavior while using valid RED thresholds:

- `MinTh = 1.0`
- `MaxTh = 2.0`
- `red_min_threshold_ppb = 500000000`
- `red_max_threshold_ppb = 1000000000`
- `red_rand_min_ppb = 1000000000`

Commands:

```bash
cd /Users/bli/Playground/ns-3.47
./ns3 build leanguard-red-retuned-bug
./ns3 run 'leanguard-red-retuned-bug --out=/tmp/ns3-347-leanguard-red-retuned-bug.csv' --no-build
/Users/bli/Playground/days/lean/.lake/build/bin/aqm_check /tmp/ns3-347-leanguard-red-retuned-bug.csv
```

Generated decision rows:

```text
0,0,decision,0,0,0,0,1020,enqueue,2,packets,0,0,ect0,ect0,red_ecn,1000000000,500000000,1000000000,1000000000,0,0,1000000000
0,1,decision,0,0,1,0,1020,enqueue,2,packets,1,1020,ect0,ect0,red_ecn,1000000000,500000000,1000000000,1000000000,1,0,1000000000
0,2,decision,0,0,2,0,1020,mark_ecn,2,packets,2,2040,ect0,ce,red_ecn,1000000000,500000000,1000000000,1000000000,2,0,1000000000
0,3,decision,0,0,2,0,1020,drop,2,packets,2,2040,ce,ce,red_ecn,1000000000,500000000,1000000000,1000000000,2,0,1000000000
```

Checker result:

```text
REJECT: line 4: missing RED witness
```

The rejected row is the `mark_ecn` decision where `queue_length=2` and
`capacity=2`. Under the corrected RED/ECN contract, physical overflow is checked
before RED marking, so the only valid action is `drop`.

Isolation check:

```bash
awk 'NR!=4' /tmp/ns3-347-leanguard-red-retuned-bug.csv \
  > /tmp/ns3-347-leanguard-red-retuned-no-mark.csv
/Users/bli/Playground/days/lean/.lake/build/bin/aqm_check /tmp/ns3-347-leanguard-red-retuned-no-mark.csv
```

Result:

```text
ACCEPT
```

This isolates the ns-3.47 incompatibility to the full-queue ECN mark decision.
