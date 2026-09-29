#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="$REPO_ROOT"

usage() {
  cat <<'EOF'
Run dictionary and panel QA checks against existing Access-derived outputs.

This is the normal "tell me whether the current generated build still looks trustworthy" wrapper.

Usage:
  bash Scripts/QA_QC/qc_only.sh

Environment:
  IPEDSDB_ROOT  External data root

This wrapper expects the main panel artifacts to already exist.
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

# Resolve both organized and legacy roots through the same Python layout helper.
LAYOUT_TEXT="$(python3 - "$REPO_ROOT/Scripts" <<'PY_LAYOUT'
import sys
sys.path.insert(0, sys.argv[1])
from access_build_utils import data_layout
layout = data_layout()
for path in (layout.root, layout.dictionary, layout.panels, layout.checks):
    print(path)
PY_LAYOUT
)"
LAYOUT_PATHS=()
while IFS= read -r path; do
  LAYOUT_PATHS+=("$path")
done <<< "$LAYOUT_TEXT"
export IPEDSDB_ROOT="${LAYOUT_PATHS[0]}"
DICT_LAKE="${LAYOUT_PATHS[1]}/dictionary_lake.parquet"
WIDE_RAW="${LAYOUT_PATHS[2]}/panel_wide_analysis_2004_2023.parquet"
WIDE_CLEAN="${LAYOUT_PATHS[2]}/panel_clean_analysis_2004_2023.parquet"
CHECKS_ROOT="${LAYOUT_PATHS[3]}"

echo "[ipedsdb-panel] QA root: $IPEDSDB_ROOT"
echo "[ipedsdb-panel] checking required build inputs"

check_path() {
  local label="$1"
  local path="$2"
  if [[ -e "$path" ]]; then
    echo "[ok] $label: $path"
  else
    echo "[error] $label not found: $path"
    exit 1
  fi
}

check_path "Data root" "$IPEDSDB_ROOT"
check_path "Dictionary lake" "$DICT_LAKE"
check_path "Wide raw panel" "$WIDE_RAW"
check_path "Wide clean panel" "$WIDE_CLEAN"

mkdir -p \
  "$CHECKS_ROOT/dictionary_qc" \
  "$CHECKS_ROOT/panel_qc" \
  "$CHECKS_ROOT/acceptance_qc"

python3 "$ROOT/Scripts/QA_QC/00_dictionary_qaqc.py" --root "$IPEDSDB_ROOT"
python3 "$ROOT/Scripts/QA_QC/01_panel_qa.py" \
  --raw "$WIDE_RAW" \
  --clean "$WIDE_CLEAN" \
  --out-dir "$CHECKS_ROOT/panel_qc" \
  --prch-qc-dir "$CHECKS_ROOT/prch_qc"
python3 "$ROOT/Scripts/QA_QC/09_panel_structure_qc.py" \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023" \
  --out-dir "$CHECKS_ROOT/panel_qc"
python3 "$ROOT/Scripts/QA_QC/08_acceptance_audit.py" \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023" \
  --out-dir "$CHECKS_ROOT/acceptance_qc"

echo ""
echo "[ipedsdb-panel] QA complete"
echo "QC outputs written to:"
echo "  $CHECKS_ROOT/dictionary_qc"
echo "  $CHECKS_ROOT/panel_qc"
echo "  $CHECKS_ROOT/acceptance_qc"
echo ""
echo "Open these first:"
echo "  $CHECKS_ROOT/acceptance_qc/acceptance_summary.md"
echo "  $CHECKS_ROOT/panel_qc/panel_qa_summary.csv"
echo "  $CHECKS_ROOT/panel_qc/panel_structure_summary.csv"
