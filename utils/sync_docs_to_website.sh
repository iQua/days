#!/usr/bin/env bash
set -euo pipefail

DAYS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEBSITE_DIR="${DAYS_WEBSITE_DIR:-"$DAYS_DIR/../days-website"}"

if [[ ! -d "$WEBSITE_DIR" ]]; then
  echo "ERROR: days-website repo not found at: $WEBSITE_DIR" >&2
  echo "Set DAYS_WEBSITE_DIR to override." >&2
  exit 1
fi

MKDOCS_BIN="${MKDOCS_BIN:-}"
if [[ -z "$MKDOCS_BIN" ]]; then
  if [[ -x "$DAYS_DIR/.venv/bin/mkdocs" ]]; then
    MKDOCS_BIN="$DAYS_DIR/.venv/bin/mkdocs"
  else
    MKDOCS_BIN="mkdocs"
  fi
fi

"$MKDOCS_BIN" build -f "$DAYS_DIR/docs/mkdocs.yml"

mkdir -p "$WEBSITE_DIR/public/docs"
rsync -a --delete "$DAYS_DIR/docs/site/" "$WEBSITE_DIR/public/docs/"

echo "Synced MkDocs site to: $WEBSITE_DIR/public/docs/"

