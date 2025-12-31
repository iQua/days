# Logging & Reports

Days writes periodic CSV reports through `CsvLogger` (`src/utils/logger.rs`).

## How reporting works

- Components construct report structs (e.g., `PacketSourceReport`, `SchedulerReport`).
- Reports are appended to an in-memory buffer (guarded by a lock).
- When buffers grow beyond a threshold, the logger flushes a batch to disk.
- At the end of the run, `CsvLogger::flush_reports()` flushes all remaining data.

The logger is initialized once from config at the start of `Topology::run`.

## Output files

All files are written under `log_path` (default: `./output/`):

- `sources.csv`: source reports (`PacketSourceReport`)
- `sinks.csv`: sink reports (`PacketSinkReport`)
- `switches.csv`: scheduler reports (`SchedulerReport`)
- `pfc.csv`: PFC ingress port reports (`PfcPortReport`, only with `l2_pfc`)
- `dcqcn_events.csv`: per-event DCQCN trace rows (only with `dcqcn` + `lean`)
- `pfc_events.csv`: per-event PFC trace rows (only with `l2_pfc` + `lean`)
- `cubic_events.csv`: per-event TCP CUBIC trace rows (only with `lean`)

## Report interval

Set `report_interval` in the TOML config:

- if omitted, it defaults to `f64::MAX` (effectively “only final flush”),
- if set, many models schedule periodic events to emit reports.

See `configuration/logging.md` for configuration details.
