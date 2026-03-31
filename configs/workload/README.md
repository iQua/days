# Workload Config Layout

This directory stores workload-specific simulation configs.

- `p2p/`: point-to-point workload configs, split by source stage:
  - `p2p/training/`
  - `p2p/inference/`
- `collective/`: collective workload configs, including the default end-to-end
  evaluation scenarios:
  - `collective/training/`
  - `collective/inference/`

Current defaults:

- `fct-to-days --workload-name <name> --class training` writes TOML to
  `configs/workload/p2p/training/<name>.toml`.
- `fct-to-days --workload-name <name> --class inference` writes TOML to
  `configs/workload/p2p/inference/<name>.toml`.
- `--workload-name` is required so generated p2p files keep the same scenario
  naming style as collective configs.
- The same command also copies the raw input FCT into
  `workload/p2p/<class>/<name>_fct.txt` and writes TSV to
  `workload/p2p/<class>/<name>.tsv`.
- `p2p-workload-import` examples should target either
  `configs/workload/p2p/training/` or `configs/workload/p2p/inference/`.
- `run_days.sh` and `eval-runner` read scenario TOMLs from
  `configs/workload/collective/` by default.
