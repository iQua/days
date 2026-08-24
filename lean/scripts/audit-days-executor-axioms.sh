#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"
cd "$lean_dir"

set +e
matches="$(
  rg --line-number --glob '*.lean' \
    '(^|[^[:alnum:]_])(sorry|admit|native_decide)([^[:alnum:]_]|$)' \
    DaysExecutor DaysExecutor.lean
)"
scan_result=$?
set -e

case "$scan_result" in
  0)
    echo "Forbidden Lean placeholder or native decision found:" >&2
    printf '%s\n' "$matches" >&2
    exit 1
    ;;
  1)
    echo "PASS forbidden-token scan"
    ;;
  *)
    echo "Failed to scan DaysExecutor Lean sources" >&2
    exit "$scan_result"
    ;;
esac

lake build DaysExecutor
lake env lean DaysExecutorAxiomAudit.lean
