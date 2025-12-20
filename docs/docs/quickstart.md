# Quickstart

## Run the built-in config examples

The repository ships with runnable TOML configs in `configs/`.

```bash
RUST_LOG=info cargo run -- configs/simple.toml
```

Useful variations:

```bash
# TCP example
RUST_LOG=info cargo run -- configs/tcp_simple.toml

# Fat-tree topology examples
RUST_LOG=info cargo run -- configs/fattree.toml
RUST_LOG=info cargo run -- configs/tcp_fattree.toml
RUST_LOG=info cargo run -- configs/bbr_fattree.toml
```

## Output artifacts

By default, Days writes periodic reports to CSV files under `log_path` (see `configuration/logging.md`):

- `sources.csv`: per-source send statistics
- `sinks.csv`: per-sink receive statistics
- `switches.csv`: per-scheduler (port) statistics
- `pfc.csv`: PFC port statistics (only with `--features l2_pfc`)
- `dcqcn_events.csv`: DCQCN event trace (only with `--features dcqcn,lean`)

## Debugging

Increase log verbosity with `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- configs/simple.toml
```

## Mental model

At a high level, Days runs this pipeline for each flow:

`PacketSource -> (host) PacketSwitch -> [Scheduler per hop] -> ... -> PacketSwitch -> PacketSink`

Packets carry the current simulation timestamp (`Packet.time`) and accumulate queueing delay. Most components avoid querying the global simulation clock, using packet timestamps instead (see `architecture/time-concurrency.md`).
