#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export IPEDSDB_ROOT="${IPEDSDB_ROOT:-/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling}"

usage() {
  cat <<'EOF'
Run the full IPEDS Access-database panel pipeline.

This is the normal "do the real build" wrapper.

Usage:
  bash manual_commands.sh

Environment:
  IPEDSDB_ROOT  External data root (default: /Users/markjaysonfarol13/Projects/IPEDSDB_Paneling)

System dependency:
  mdb-tables, mdb-schema, mdb-export

Outputs:
  $IPEDSDB_ROOT/Panels/2004-2023/panel_long_varnum_2004_2023.parquet
  $IPEDSDB_ROOT/Panels/panel_wide_analysis_2004_2023.parquet
  $IPEDSDB_ROOT/Panels/panel_clean_analysis_2004_2023.parquet

What this wrapper does:
  1. activates .venv if present
  2. verifies mdb-tools
  3. runs the full 2004:2023 pipeline
  4. runs cleaning and QA

Best use:
  use this when you want the repo to produce or refresh the full release-style panel
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT/.venv/bin/activate"
fi

echo "[ipedsdb-panel] repo: $ROOT"
echo "[ipedsdb-panel] data root: $IPEDSDB_ROOT"
echo "[ipedsdb-panel] starting preflight"

for bin in mdb-tables mdb-schema mdb-export; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required system dependency: $bin" >&2
    echo "Preflight failed. Install mdb-tools before running the pipeline." >&2
    exit 1
  fi
done

mkdir -p "$IPEDSDB_ROOT"

echo "[ipedsdb-panel] preflight passed"
echo "[ipedsdb-panel] running full pipeline for years 2004:2023"
echo "[ipedsdb-panel] this can take a while on a first run"

python3 "$ROOT/Scripts/00_run_all.py" \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023" \
  --run-cleaning \
  --run-qaqc

echo ""
echo "[ipedsdb-panel] run complete"
echo "Local outputs:"
echo "  $IPEDSDB_ROOT/Panels/2004-2023/panel_long_varnum_2004_2023.parquet"
echo "  $IPEDSDB_ROOT/Panels/panel_wide_analysis_2004_2023.parquet"
echo "  $IPEDSDB_ROOT/Panels/panel_clean_analysis_2004_2023.parquet"
echo ""
echo "Recommended next checks:"
echo "  $IPEDSDB_ROOT/Checks/dictionary_qc/dictionary_qaqc_summary.csv"
echo "  $IPEDSDB_ROOT/Checks/panel_qc/panel_qa_summary.csv"
echo "  $IPEDSDB_ROOT/Checks/panel_qc/panel_structure_summary.csv"
echo "  $IPEDSDB_ROOT/Checks/acceptance_qc/acceptance_summary.md"
