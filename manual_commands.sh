#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_IPEDSDB_ROOT="/Volumes/CIRAGO/IPEDSDB_PANEL"
export IPEDSDB_ROOT="${IPEDSDB_ROOT:-$DEFAULT_IPEDSDB_ROOT}"

usage() {
  cat <<'EOF'
Run the full IPEDS Access-database panel pipeline.

This is the normal "do the real build" wrapper.

Usage:
  bash manual_commands.sh

Environment:
  IPEDSDB_ROOT  External data root (default: /Volumes/CIRAGO/IPEDSDB_PANEL)

System dependency:
  mdb-tables, mdb-schema, mdb-export

Draft outputs for organized roots (layout.json):
  $IPEDSDB_ROOT/Work/Panels/2004-2023/panel_long_varnum_2004_2023.parquet
  $IPEDSDB_ROOT/Work/Panels/panel_wide_analysis_2004_2023.parquet
  $IPEDSDB_ROOT/Work/Panels/panel_clean_analysis_2004_2023.parquet

Verified analyst panels live in Final. This build does not publish to Final.
Roots without layout.json retain the legacy Panels/ and Checks/ paths.

What this wrapper does:
  1. activates .venv if present
  2. verifies mdb-tools
  3. runs the full 2004:2023 pipeline
  4. runs cleaning and QA

Best use:
  use this to build and check draft panels before separate verified publication
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

python3 - "$ROOT" "$IPEDSDB_ROOT" <<'PY'
import sys
sys.path.insert(0, sys.argv[1] + "/Scripts")
from access_build_utils import require_data_volume
require_data_volume(sys.argv[2])
PY

OUTPUT_BASE="$IPEDSDB_ROOT"
if [[ -f "$IPEDSDB_ROOT/layout.json" ]]; then
  OUTPUT_BASE="$IPEDSDB_ROOT/Work"
fi

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
echo "Draft outputs (Final requires separate verified publication):"
echo "  $OUTPUT_BASE/Panels/2004-2023/panel_long_varnum_2004_2023.parquet"
echo "  $OUTPUT_BASE/Panels/panel_wide_analysis_2004_2023.parquet"
echo "  $OUTPUT_BASE/Panels/panel_clean_analysis_2004_2023.parquet"
echo ""
echo "Recommended next checks:"
echo "  $OUTPUT_BASE/Checks/dictionary_qc/dictionary_qaqc_summary.csv"
echo "  $OUTPUT_BASE/Checks/panel_qc/panel_qa_summary.csv"
echo "  $OUTPUT_BASE/Checks/panel_qc/panel_structure_summary.csv"
echo "  $OUTPUT_BASE/Checks/acceptance_qc/acceptance_summary.md"
