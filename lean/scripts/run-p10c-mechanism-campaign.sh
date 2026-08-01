#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_mechanisms_check
.lake/build/bin/p10c_mechanisms_check
