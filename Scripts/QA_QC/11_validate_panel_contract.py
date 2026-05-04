#!/usr/bin/env python3
"""
QA 11: validate the declared panel contract against current code policy.

Reads:
- `contracts/panel_spec.toml`
- wide-build parser defaults
- shared PRCH policy module

Writes:
- no files; exits nonzero if the contract drifts from code defaults or the
  shared cleaning policy
"""
from __future__ import annotations

import argparse
import csv
import sys
import tomllib
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "Scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from prch_policy import POLICY_BY_FLAG  # noqa: E402
from wide_build_common import build_arg_parser  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--contract", default=str(REPO_ROOT / "contracts" / "panel_spec.toml"))
    return p.parse_args()


def load_contract(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def parser_defaults() -> dict[str, Any]:
    parser = build_arg_parser(REPO_ROOT)
    defaults: dict[str, Any] = {}
    for action in parser._actions:
        if not action.dest or action.dest == "help":
            continue
        defaults[action.dest] = action.default
    return defaults


def csv_list(value: str | None) -> list[str]:
    if value is None:
        return []
    return [part.strip().upper() for part in str(value).split(",") if part.strip()]


def fail(rows: list[str], message: str) -> None:
    rows.append(message)


def compare_value(rows: list[str], label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        fail(rows, f"{label}: contract={actual!r} code={expected!r}")


def validate_wide_build(contract: dict[str, Any], rows: list[str]) -> None:
    wide = contract.get("wide_build", {})
    defaults = parser_defaults()
    compare_value(rows, "wide_build.dim_sources", wide.get("dim_sources"), csv_list(defaults.get("dim_sources")))
    compare_value(rows, "wide_build.dim_prefixes", wide.get("dim_prefixes"), csv_list(defaults.get("dim_prefixes")))
    compare_value(rows, "wide_build.anti_garbage_ids", wide.get("anti_garbage_ids"), csv_list(defaults.get("anti_garbage_ids")))
    for key in [
        "fail_on_scalar_conflicts",
        "drop_anti_garbage_cols",
        "fail_on_anti_garbage",
        "drop_globally_null_post",
        "legacy_analysis_schema",
        "scalar_conflict_buckets",
        "scalar_conflict_bucket_min_year",
        "disc_suffix",
    ]:
        compare_value(rows, f"wide_build.{key}", wide.get(key), defaults.get(key))


def normalize_policy_rows(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in contract.get("prch_policy", []):
        flag = str(row.get("flag", "")).strip().upper()
        if flag:
            out[flag] = row
    return out


def validate_prch_policy(contract: dict[str, Any], rows: list[str]) -> None:
    declared = normalize_policy_rows(contract)
    expected_flags = set(POLICY_BY_FLAG)
    declared_flags = set(declared)
    missing = sorted(expected_flags - declared_flags)
    extra = sorted(declared_flags - expected_flags)
    if missing:
        fail(rows, f"prch_policy missing flags: {', '.join(missing)}")
    if extra:
        fail(rows, f"prch_policy extra flags: {', '.join(extra)}")
    for flag, policy in sorted(POLICY_BY_FLAG.items()):
        row = declared.get(flag)
        if row is None:
            continue
        compare_value(rows, f"prch_policy.{flag}.cleaned_child_codes", row.get("cleaned_child_codes", []), list(policy.cleaned_child_codes))
        compare_value(rows, f"prch_policy.{flag}.review_only_codes", row.get("review_only_codes", []), list(policy.review_only_codes))
        compare_value(rows, f"prch_policy.{flag}.target_source_files", row.get("target_source_files", []), list(policy.target_source_files))
        compare_value(rows, f"prch_policy.{flag}.target_source_prefixes", row.get("target_source_prefixes", []), list(policy.target_source_prefixes))


def validate_release(contract: dict[str, Any], rows: list[str]) -> None:
    release = contract.get("release", {})
    compare_value(rows, "release.default_year_spec", release.get("default_year_spec"), "2004:2023")
    compare_value(rows, "release.start_year", release.get("start_year"), 2004)
    compare_value(rows, "release.end_year", release.get("end_year"), 2023)
    compare_value(rows, "release.final_only", release.get("final_only"), True)
    compare_value(rows, "release.include_provisional", release.get("include_provisional"), False)


def validate_qa_artifacts(contract: dict[str, Any], rows: list[str]) -> None:
    qa = contract.get("qa", {})
    expected = {
        "dictionary_qc": "Checks/dictionary_qc/dictionary_qaqc_summary.csv",
        "panel_qc": "Checks/panel_qc/panel_qa_summary.csv",
        "panel_structure_qc": "Checks/panel_qc/panel_structure_summary.csv",
        "acceptance_summary": "Checks/acceptance_qc/acceptance_summary.csv",
        "release_metrics": "Checks/release_metrics/table_release_validation_metrics_filled.csv",
        "release_manifest": "Checks/release_manifest/release_manifest.csv",
        "release_manifest_verification": "Checks/release_manifest/release_manifest_verification.csv",
        "release_bundle_manifest": "Releases/ipedsdb_panel_2004_2023/bundle_manifest.csv",
        "release_comparison": "Checks/release_compare/release_comparison.csv",
        "datapackage": "Checks/release_metadata/datapackage.json",
        "build_provenance": "Checks/release_metadata/build_provenance.json",
    }
    for key, value in expected.items():
        compare_value(rows, f"qa.{key}", qa.get(key), value)


def validate_dictionary_overrides(contract: dict[str, Any], rows: list[str]) -> None:
    inputs = contract.get("inputs", {})
    rel_path = inputs.get("dictionary_ambiguity_overrides")
    if not rel_path:
        fail(rows, "inputs.dictionary_ambiguity_overrides is missing")
        return
    path = REPO_ROOT / str(rel_path)
    if not path.exists():
        fail(rows, f"dictionary ambiguity override file does not exist: {rel_path}")
        return
    required = {
        "year",
        "source_file",
        "access_table_name",
        "varname",
        "selected_varnumber",
        "selected_metadata_source",
        "selected_metadata_table_name",
        "justification",
    }
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = set(reader.fieldnames or [])
        missing = required - header
        if missing:
            fail(rows, f"dictionary ambiguity override file missing columns: {', '.join(sorted(missing))}")
        for idx, row in enumerate(reader, start=2):
            if not any((value or "").strip() for value in row.values()):
                continue
            if not (row.get("source_file", "").strip() or row.get("access_table_name", "").strip()):
                fail(rows, f"dictionary ambiguity override row {idx} lacks source_file/access_table_name")
            if not (
                row.get("selected_varnumber", "").strip()
                or row.get("selected_metadata_source", "").strip()
                or row.get("selected_metadata_table_name", "").strip()
            ):
                fail(rows, f"dictionary ambiguity override row {idx} lacks selected_* field")
            if not row.get("justification", "").strip():
                fail(rows, f"dictionary ambiguity override row {idx} lacks justification")


def validate_release_governance_files(rows: list[str]) -> None:
    checks = {
        "contracts/release_diff_overrides.csv": {
            "category",
            "artifact_key",
            "relative_path",
            "field",
            "baseline",
            "current",
            "justification",
        },
        "contracts/known_limitations.csv": {
            "limitation_id",
            "scope",
            "status",
            "first_release",
            "last_reviewed",
            "description",
            "evidence_path",
            "mitigation",
        },
        "contracts/transformation_ledger.csv": {
            "transformation_id",
            "stage",
            "scope",
            "status",
            "first_release",
            "description",
            "evidence_path",
            "release_policy",
            "reviewer_note",
        },
        "contracts/external_benchmarks.csv": {
            "benchmark_id",
            "year",
            "metric",
            "column",
            "expected_value",
            "tolerance_abs",
            "tolerance_rel",
            "source",
            "notes",
        },
    }
    for rel_path, required in checks.items():
        path = REPO_ROOT / rel_path
        if not path.exists():
            fail(rows, f"governance file does not exist: {rel_path}")
            continue
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            header = set(reader.fieldnames or [])
            missing = required - header
            if missing:
                fail(rows, f"{rel_path} missing columns: {', '.join(sorted(missing))}")
            for idx, row in enumerate(reader, start=2):
                if not any((value or "").strip() for value in row.values()):
                    continue
                if "justification" in required and not row.get("justification", "").strip():
                    fail(rows, f"{rel_path} row {idx} lacks justification")
                if "limitation_id" in required and not row.get("limitation_id", "").strip():
                    fail(rows, f"{rel_path} row {idx} lacks limitation_id")
                if "transformation_id" in required and not row.get("transformation_id", "").strip():
                    fail(rows, f"{rel_path} row {idx} lacks transformation_id")


def main() -> int:
    args = parse_args()
    contract_path = Path(args.contract)
    contract = load_contract(contract_path)
    failures: list[str] = []
    validate_release(contract, failures)
    validate_qa_artifacts(contract, failures)
    validate_wide_build(contract, failures)
    validate_prch_policy(contract, failures)
    validate_dictionary_overrides(contract, failures)
    validate_release_governance_files(failures)
    if failures:
        print("Panel contract validation failed:")
        for row in failures:
            print(f"  - {row}")
        return 1
    print(f"Panel contract validation passed: {contract_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
