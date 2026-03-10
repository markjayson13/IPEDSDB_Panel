#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export IPEDSDB_ROOT="${IPEDSDB_ROOT:-/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling}"

usage() {
  cat <<'EOF'
Run the full IPEDS Access-database panel pipeline.

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

for bin in mdb-tables mdb-schema mdb-export; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required system dependency: $bin" >&2
    echo "Preflight failed. Install mdb-tools before running the pipeline." >&2
    exit 1
  fi
done

mkdir -p "$IPEDSDB_ROOT"

python3 "$ROOT/Scripts/00_run_all.py" \
  --root "$IPEDSDB_ROOT" \
  --years "2004:2023" \
  --run-cleaning \
  --run-qaqc

echo ""
echo "Local outputs:"
echo "  $IPEDSDB_ROOT/Panels/2004-2023/panel_long_varnum_2004_2023.parquet"
echo "  $IPEDSDB_ROOT/Panels/panel_wide_analysis_2004_2023.parquet"
echo "  $IPEDSDB_ROOT/Panels/panel_clean_analysis_2004_2023.parquet"
