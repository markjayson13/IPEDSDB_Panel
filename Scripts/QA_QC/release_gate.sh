#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_IPEDSDB_ROOT="$(dirname "$REPO_ROOT")/IPEDSDB_ROOT"
ROOT="${IPEDSDB_ROOT:-$DEFAULT_IPEDSDB_ROOT}"
YEARS="${YEARS:-2004:2023}"
BASELINE_MANIFEST="${BASELINE_MANIFEST:-}"
BASELINE_ROOT="${BASELINE_ROOT:-}"
REQUIRE_EXTERNAL_BENCHMARKS="${REQUIRE_EXTERNAL_BENCHMARKS:-0}"
cd "$REPO_ROOT"

python3 Scripts/QA_QC/11_validate_panel_contract.py
python3 Scripts/QA_QC/18_public_release_guard.py
python3 Scripts/QA_QC/19_docs_style_guard.py
python3 Scripts/QA_QC/08_acceptance_audit.py --root "$ROOT" --years "$YEARS"
python3 Scripts/QA_QC/12_build_release_manifest.py --root "$ROOT" --years "$YEARS"
python3 Scripts/QA_QC/13_verify_release_manifest.py \
  --manifest "$ROOT/Checks/release_manifest/release_manifest.csv" \
  --root "$ROOT"
python3 Scripts/QA_QC/16_build_datapackage.py \
  --root "$ROOT" \
  --years "$YEARS"
python3 Scripts/QA_QC/17_build_provenance.py \
  --root "$ROOT"
python3 Scripts/QA_QC/20_environment_report.py \
  --root "$ROOT"
python3 Scripts/QA_QC/22_build_entity_continuity_crosswalk.py \
  --root "$ROOT" \
  --years "$YEARS"
benchmark_args=(
  --root "$ROOT"
  --years "$YEARS"
)
if [[ "$REQUIRE_EXTERNAL_BENCHMARKS" == "1" ]]; then
  benchmark_args+=(--require-benchmarks)
fi
python3 Scripts/QA_QC/21_external_benchmark_reconciliation.py "${benchmark_args[@]}"

if [[ -n "$BASELINE_MANIFEST" ]]; then
  args=(
    --baseline-manifest "$BASELINE_MANIFEST"
    --current-manifest "$ROOT/Checks/release_manifest/release_manifest.csv"
    --current-root "$ROOT"
    --out-dir "$ROOT/Checks/release_compare"
  )
  if [[ -n "$BASELINE_ROOT" ]]; then
    args+=(--baseline-root "$BASELINE_ROOT")
  fi
  python3 Scripts/QA_QC/15_compare_release_to_baseline.py "${args[@]}"
fi

python3 Scripts/QA_QC/14_build_public_release_bundle.py \
  --root "$ROOT" \
  --years "$YEARS" \
  --out-dir "$ROOT/Releases/ipedsdb_panel_${YEARS/:/_}"
python3 Scripts/QA_QC/05_repo_size_guard.py
python3 Scripts/QA_QC/06_staged_repo_guard.py
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q
