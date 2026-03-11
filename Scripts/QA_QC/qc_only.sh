#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export IPEDSDB_ROOT="${IPEDSDB_ROOT:-/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling}"

usage() {
  cat <<'EOF'
Run dictionary and panel QA checks against existing Access-derived outputs.

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

echo "[ipedsdb-panel] QA root: $IPEDSDB_ROOT"
echo "[ipedsdb-panel] checking required inputs"

DICT_LAKE="$IPEDSDB_ROOT/Dictionary/dictionary_lake.parquet"
WIDE_RAW="$IPEDSDB_ROOT/Panels/panel_wide_analysis_2004_2023.parquet"
WIDE_CLEAN="$IPEDSDB_ROOT/Panels/panel_clean_analysis_2004_2023.parquet"

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
  "$IPEDSDB_ROOT/Checks/dictionary_qc" \
  "$IPEDSDB_ROOT/Checks/panel_qc"

python3 "$ROOT/Scripts/QA_QC/00_dictionary_qaqc.py" --root "$IPEDSDB_ROOT"
python3 "$ROOT/Scripts/QA_QC/01_panel_qa.py" \
  --raw "$WIDE_RAW" \
  --clean "$WIDE_CLEAN" \
  --out-dir "$IPEDSDB_ROOT/Checks/panel_qc" \
  --prch-qc-dir "$IPEDSDB_ROOT/Checks/prch_qc"

echo ""
echo "[ipedsdb-panel] QA complete"
echo "QC outputs written to:"
echo "  $IPEDSDB_ROOT/Checks/dictionary_qc"
echo "  $IPEDSDB_ROOT/Checks/panel_qc"
