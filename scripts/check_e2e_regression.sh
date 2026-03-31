#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_TS="$(date '+%Y%m%d_%H%M%S')"
RESULT_DIR="${ROOT_DIR}/results/evaluation/regression_${RUN_TS}"
SUMMARY_CSV="${RESULT_DIR}/summary.csv"
SCENARIOS=()

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/check_e2e_regression.sh [--scenario <name>]...

Options:
  --scenario <name>   Run only one or more named scenarios (repeatable).
  -h, --help          Show this help.

Description:
  Runs native days evaluation configs and validates output artifacts:
  - sinks.csv / sources.csv / switches.csv / traces.json exist
  - records CSV line counts (files may be empty for some scenarios)
  - traces.json is valid JSON
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)
      SCENARIOS+=( "$2" )
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "${RESULT_DIR}" "${RESULT_DIR}/run_configs"

echo "scenario,status,sinks_rows,sources_rows,switches_rows,log_path,config_path" > "${SUMMARY_CSV}"

CONFIG_FILES=()
while IFS= read -r cfg_path; do
  [[ -n "${cfg_path}" ]] || continue
  CONFIG_FILES+=( "${cfg_path}" )
done < <(find "${ROOT_DIR}/configs/workload/collective" -mindepth 1 -maxdepth 2 -name '*.toml' | sort)

if [[ "${#CONFIG_FILES[@]}" -eq 0 ]]; then
  echo "no scenario configs found under ${ROOT_DIR}/configs/workload/collective" >&2
  exit 1
fi

matches_filter() {
  local scenario_name="$1"
  if [[ "${#SCENARIOS[@]}" -eq 0 ]]; then
    return 0
  fi
  local s
  for s in "${SCENARIOS[@]}"; do
    if [[ "${scenario_name}" == "${s}" ]]; then
      return 0
    fi
  done
  return 1
}

extract_name() {
  local cfg="$1"
  python3 - "$cfg" <<'PY'
import pathlib
import sys
import tomllib
p = pathlib.Path(sys.argv[1])
data = tomllib.loads(p.read_text(encoding="utf-8"))
name = data.get("name")
if isinstance(name, str) and name.strip():
    print(name.strip())
else:
    print(p.stem)
PY
}

build_run_config() {
  local src_cfg="$1"
  local dst_cfg="$2"
  local run_log_path="$3"

  awk -v run_log_path="${run_log_path}" '
    BEGIN { replaced = 0 }
    /^[[:space:]]*log_path[[:space:]]*=/ {
      if (!replaced) {
        print "log_path = \"" run_log_path "\""
        replaced = 1
      }
      next
    }
    { print }
    END {
      if (!replaced) {
        print ""
        print "log_path = \"" run_log_path "\""
      }
    }
  ' "${src_cfg}" > "${dst_cfg}"
}

validate_outputs() {
  local log_dir="$1"
  python3 - "$log_dir" <<'PY'
import json
import pathlib
import sys

log_dir = pathlib.Path(sys.argv[1])
required = ["sinks.csv", "sources.csv", "switches.csv", "traces.json"]
for fn in required:
    p = log_dir / fn
    if not p.is_file():
        raise SystemExit(f"missing artifact: {p}")

counts = {}
for fn in ["sinks.csv", "sources.csv", "switches.csv"]:
    p = log_dir / fn
    text = p.read_text(encoding="utf-8")
    counts[fn] = len(text.splitlines())

with (log_dir / "traces.json").open("r", encoding="utf-8") as f:
    json.load(f)

print(f"{counts['sinks.csv']},{counts['sources.csv']},{counts['switches.csv']}")
PY
}

failures=0
ran=0

for cfg in "${CONFIG_FILES[@]}"; do
  scenario_name="$(extract_name "${cfg}")"
  if ! matches_filter "${scenario_name}"; then
    continue
  fi

  ran=$((ran + 1))
  run_name="${scenario_name}_${RUN_TS}"
  run_cfg="${RESULT_DIR}/run_configs/${run_name}.toml"
  run_log_path="${RESULT_DIR}/${run_name}.logs"

  echo "[run] ${scenario_name}"
  build_run_config "${cfg}" "${run_cfg}" "${run_log_path}"

  run_profile_log="${RESULT_DIR}/${run_name}.profile.log"
  if cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- "${run_cfg}" > "${run_profile_log}" 2>&1; then
    if grep -Eqi "simulation stopped early|deadlock" "${run_profile_log}"; then
      failures=$((failures + 1))
      echo "${scenario_name},failed,NA,NA,NA,${run_log_path},${run_cfg}" >> "${SUMMARY_CSV}"
      echo "[fail] ${scenario_name} runtime reported deadlock/early-stop" >&2
    elif counts="$(validate_outputs "${run_log_path}")"; then
      sinks_rows="${counts%%,*}"
      rest="${counts#*,}"
      sources_rows="${rest%%,*}"
      switches_rows="${rest##*,}"
      echo "${scenario_name},ok,${sinks_rows},${sources_rows},${switches_rows},${run_log_path},${run_cfg}" >> "${SUMMARY_CSV}"
      echo "[ok] ${scenario_name} sinks=${sinks_rows} sources=${sources_rows} switches=${switches_rows}"
    else
      failures=$((failures + 1))
      echo "${scenario_name},failed,NA,NA,NA,${run_log_path},${run_cfg}" >> "${SUMMARY_CSV}"
      echo "[fail] ${scenario_name} output validation failed" >&2
    fi
  else
    failures=$((failures + 1))
    echo "${scenario_name},failed,NA,NA,NA,${run_log_path},${run_cfg}" >> "${SUMMARY_CSV}"
    echo "[fail] ${scenario_name} run failed" >&2
  fi

done

if [[ "${ran}" -eq 0 ]]; then
  echo "no scenarios selected" >&2
  exit 1
fi

echo

echo "summary: ${SUMMARY_CSV}"
if [[ "${failures}" -ne 0 ]]; then
  echo "failed scenarios: ${failures}" >&2
  exit 1
fi
