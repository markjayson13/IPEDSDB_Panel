#!/usr/bin/env python3
"""
QA 15: compare a current release against a baseline release.

Reads:
- baseline `release_manifest.csv`
- current `release_manifest.csv`
- `contracts/release_diff_overrides.csv`
- parquet and CSV artifacts when both release roots are available

Writes:
- `release_comparison.csv`
- `release_comparison_summary.csv`
- `release_comparison.md`

The comparison is a release gate. It reports all detected differences and exits
nonzero when failing differences are present.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, data_layout, repo_root


CORE_ROLES = {"panel_output", "dictionary_artifact", "long_year_output"}
PANEL_ROLES = {"panel_output", "long_year_output"}
DICTIONARY_REL = "Dictionary/dictionary_lake.parquet"
PRCH_TABLES = {
    "Checks/prch_qc/prch_flag_policy.csv": ["flag"],
    "Checks/prch_qc/prch_clean_summary.csv": ["year", "flag"],
    "Checks/prch_qc/prch_clean_columns.csv": ["flag", "column"],
    "Checks/prch_qc/prch_flag_code_counts.csv": ["year", "flag", "code"],
}
FAIL_RANK = {"none": 0, "review": 1, "fail": 2}
OVERRIDE_COLUMNS = [
    "category",
    "artifact_key",
    "relative_path",
    "field",
    "baseline",
    "current",
    "justification",
]


def parse_args() -> argparse.Namespace:
    current_root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--baseline-manifest", required=True, help="Baseline release_manifest.csv")
    p.add_argument("--current-manifest", default=None, help="Current release_manifest.csv. Defaults under current root.")
    p.add_argument("--baseline-root", default=None, help="Baseline IPEDSDB_ROOT. Optional if manifest absolute paths are valid.")
    p.add_argument("--current-root", default=str(current_root), help="Current IPEDSDB_ROOT")
    p.add_argument("--baseline-repo-root", default=None, help="Baseline repo root. Optional if manifest absolute paths are valid.")
    p.add_argument("--current-repo-root", default=str(repo_root()), help="Current repo root")
    p.add_argument("--out-dir", default=None, help="Output directory. Defaults to Checks/release_compare under current root.")
    p.add_argument("--overrides", default=str(repo_root() / "contracts" / "release_diff_overrides.csv"))
    p.add_argument("--fail-on", choices=["fail", "review", "none"], default="fail")
    return p.parse_args()


def boolish(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def norm(value: object) -> str:
    return str(value if value is not None else "").strip()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Required file does not exist: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def load_overrides(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = set(reader.fieldnames or [])
        missing = set(OVERRIDE_COLUMNS) - header
        if missing:
            raise SystemExit(f"Release diff override file missing columns: {', '.join(sorted(missing))}")
        rows = []
        for idx, row in enumerate(reader, start=2):
            clean = {col: norm(row.get(col, "")) for col in OVERRIDE_COLUMNS}
            if not any(clean.values()):
                continue
            if not clean["category"] and not clean["relative_path"]:
                raise SystemExit(f"Release diff override row {idx} must include category or relative_path")
            if not clean["justification"]:
                raise SystemExit(f"Release diff override row {idx} lacks justification")
            rows.append(clean)
        return rows


def artifact_key(row: pd.Series | dict[str, Any]) -> str:
    return f"{norm(row.get('path_base', ''))}:{norm(row.get('relative_path', ''))}"


def artifact_severity(row: pd.Series | dict[str, Any]) -> str:
    return "fail" if norm(row.get("role", "")) in CORE_ROLES else "review"


def add_issue(
    rows: list[dict[str, object]],
    *,
    category: str,
    severity: str,
    artifact: str,
    relative_path: str,
    field: str = "",
    baseline: object = "",
    current: object = "",
    detail: str = "",
) -> None:
    rows.append(
        {
            "category": category,
            "severity": severity,
            "artifact_key": artifact,
            "relative_path": relative_path,
            "field": field,
            "baseline": norm(baseline),
            "current": norm(current),
            "detail": detail,
        }
    )


def override_matches(issue: dict[str, object], override: dict[str, str]) -> bool:
    for col in ["category", "artifact_key", "relative_path", "field", "baseline", "current"]:
        expected = norm(override.get(col, ""))
        if expected and expected != norm(issue.get(col, "")):
            return False
    return True


def apply_overrides(issues: list[dict[str, object]], overrides: list[dict[str, str]]) -> None:
    for issue in issues:
        if norm(issue.get("severity")) != "fail":
            continue
        for override in overrides:
            if override_matches(issue, override):
                issue["severity"] = "review"
                issue["detail"] = (norm(issue.get("detail")) + " Override: " + override["justification"]).strip()
                break


def resolve_path(row: pd.Series, ipeds_root: Path | None, repo: Path | None) -> Path:
    path_base = norm(row.get("path_base", ""))
    rel = norm(row.get("relative_path", ""))
    absolute = norm(row.get("absolute_path", ""))
    if path_base == "IPEDSDB_ROOT" and ipeds_root is not None:
        return ipeds_root / rel
    if path_base == "repo" and repo is not None:
        return repo / rel
    if absolute:
        return Path(absolute)
    return Path(rel)


def present_rows(manifest: pd.DataFrame) -> dict[str, pd.Series]:
    return {artifact_key(row): row for _, row in manifest.iterrows() if boolish(row.get("present", ""))}


def compare_manifest(baseline: pd.DataFrame, current: pd.DataFrame, issues: list[dict[str, object]]) -> None:
    baseline_rows = {artifact_key(row): row for _, row in baseline.iterrows()}
    current_rows = {artifact_key(row): row for _, row in current.iterrows()}
    for key in sorted(set(baseline_rows) - set(current_rows)):
        row = baseline_rows[key]
        add_issue(
            issues,
            category="artifact_removed",
            severity=artifact_severity(row),
            artifact=key,
            relative_path=row.get("relative_path", ""),
            detail="Artifact exists in baseline manifest but not current manifest.",
        )
    for key in sorted(set(current_rows) - set(baseline_rows)):
        row = current_rows[key]
        add_issue(
            issues,
            category="artifact_added",
            severity=artifact_severity(row),
            artifact=key,
            relative_path=row.get("relative_path", ""),
            detail="Artifact exists in current manifest but not baseline manifest.",
        )
    for key in sorted(set(baseline_rows) & set(current_rows)):
        b = baseline_rows[key]
        c = current_rows[key]
        rel = c.get("relative_path", b.get("relative_path", ""))
        severity = artifact_severity(c)
        for field in ["present", "file_format", "years"]:
            if norm(b.get(field, "")) != norm(c.get(field, "")):
                add_issue(
                    issues,
                    category=f"manifest_{field}_changed",
                    severity=severity,
                    artifact=key,
                    relative_path=rel,
                    field=field,
                    baseline=b.get(field, ""),
                    current=c.get(field, ""),
                )
        for field in ["rows", "columns"]:
            if norm(b.get(field, "")) != norm(c.get(field, "")):
                add_issue(
                    issues,
                    category=f"parquet_{field}_changed",
                    severity=severity,
                    artifact=key,
                    relative_path=rel,
                    field=field,
                    baseline=b.get(field, ""),
                    current=c.get(field, ""),
                )
        if boolish(b.get("present", "")) and boolish(c.get("present", "")):
            if norm(b.get("sha256", "")) != norm(c.get("sha256", "")):
                add_issue(
                    issues,
                    category="sha256_changed",
                    severity="review",
                    artifact=key,
                    relative_path=rel,
                    field="sha256",
                    baseline=b.get("sha256", ""),
                    current=c.get("sha256", ""),
                )
            if norm(b.get("size_bytes", "")) != norm(c.get("size_bytes", "")):
                add_issue(
                    issues,
                    category="size_changed",
                    severity="review",
                    artifact=key,
                    relative_path=rel,
                    field="size_bytes",
                    baseline=b.get("size_bytes", ""),
                    current=c.get("size_bytes", ""),
                )


def parquet_schema(path: Path) -> dict[str, str]:
    schema = pq.ParquetFile(path).schema_arrow
    return {field.name: str(field.type) for field in schema}


def parquet_year_summary(path: Path) -> dict[str, str]:
    schema = pq.ParquetFile(path).schema_arrow
    if "year" not in schema.names:
        return {}
    table = pq.read_table(path, columns=["year"])
    values = pd.to_numeric(table.column("year").to_pandas(), errors="coerce").dropna()
    if values.empty:
        return {"year_min": "", "year_max": "", "year_count": "0"}
    return {
        "year_min": str(int(values.min())),
        "year_max": str(int(values.max())),
        "year_count": str(int(values.nunique())),
    }


def compare_parquet_files(
    baseline_manifest: pd.DataFrame,
    current_manifest: pd.DataFrame,
    baseline_root: Path | None,
    current_root: Path | None,
    baseline_repo: Path | None,
    current_repo: Path | None,
    issues: list[dict[str, object]],
) -> None:
    baseline_rows = present_rows(baseline_manifest)
    current_rows = present_rows(current_manifest)
    for key in sorted(set(baseline_rows) & set(current_rows)):
        b = baseline_rows[key]
        c = current_rows[key]
        if norm(c.get("file_format", "")) != "parquet":
            continue
        role = norm(c.get("role", ""))
        if role not in CORE_ROLES:
            continue
        b_path = resolve_path(b, baseline_root, baseline_repo)
        c_path = resolve_path(c, current_root, current_repo)
        rel = c.get("relative_path", "")
        if not (b_path.exists() and c_path.exists()):
            add_issue(
                issues,
                category="deep_compare_skipped",
                severity="review",
                artifact=key,
                relative_path=rel,
                detail="Baseline or current parquet file is not available on disk.",
            )
            continue
        b_schema = parquet_schema(b_path)
        c_schema = parquet_schema(c_path)
        for name in sorted(set(b_schema) - set(c_schema)):
            add_issue(
                issues,
                category="parquet_schema_column_removed",
                severity="fail",
                artifact=key,
                relative_path=rel,
                field=name,
                baseline=b_schema[name],
                current="",
            )
        for name in sorted(set(c_schema) - set(b_schema)):
            add_issue(
                issues,
                category="parquet_schema_column_added",
                severity="fail",
                artifact=key,
                relative_path=rel,
                field=name,
                baseline="",
                current=c_schema[name],
            )
        for name in sorted(set(b_schema) & set(c_schema)):
            if b_schema[name] != c_schema[name]:
                add_issue(
                    issues,
                    category="parquet_schema_type_changed",
                    severity="fail",
                    artifact=key,
                    relative_path=rel,
                    field=name,
                    baseline=b_schema[name],
                    current=c_schema[name],
                )
        if role in PANEL_ROLES:
            b_years = parquet_year_summary(b_path)
            c_years = parquet_year_summary(c_path)
            for field in sorted(set(b_years) | set(c_years)):
                if b_years.get(field, "") != c_years.get(field, ""):
                    add_issue(
                        issues,
                        category="year_coverage_changed",
                        severity="fail",
                        artifact=key,
                        relative_path=rel,
                        field=field,
                        baseline=b_years.get(field, ""),
                        current=c_years.get(field, ""),
                    )


def artifact_row(manifest: pd.DataFrame, relative_path: str) -> pd.Series | None:
    matches = manifest[(manifest["relative_path"] == relative_path) & manifest["present"].map(boolish)]
    if matches.empty:
        return None
    return matches.iloc[0]


def dictionary_mapping(path: Path) -> tuple[dict[tuple[str, ...], str], list[str]]:
    schema = pq.ParquetFile(path).schema_arrow
    names = set(schema.names)
    key_cols = [col for col in ["year", "source_file", "access_table_name", "varname"] if col in names]
    value_cols = [col for col in ["varnumber", "DataType", "metadata_source", "metadata_table_name"] if col in names]
    missing = [col for col in ["year", "source_file", "varname"] if col not in names]
    if missing or not value_cols:
        return {}, missing
    df = pd.read_parquet(path, columns=key_cols + value_cols).fillna("")
    for col in key_cols + value_cols:
        df[col] = df[col].astype(str).str.strip()
    grouped = df.groupby(key_cols, dropna=False)[value_cols].agg(lambda values: "|".join(sorted({norm(v) for v in values if norm(v)}))).reset_index()
    mapping = {
        tuple(norm(row[col]) for col in key_cols): " ; ".join(f"{col}={norm(row[col])}" for col in value_cols)
        for _, row in grouped.iterrows()
    }
    return mapping, []


def compare_dictionary(
    baseline_manifest: pd.DataFrame,
    current_manifest: pd.DataFrame,
    baseline_root: Path | None,
    current_root: Path | None,
    baseline_repo: Path | None,
    current_repo: Path | None,
    issues: list[dict[str, object]],
) -> None:
    b_row = artifact_row(baseline_manifest, DICTIONARY_REL)
    c_row = artifact_row(current_manifest, DICTIONARY_REL)
    if b_row is None or c_row is None:
        return
    key = artifact_key(c_row)
    b_path = resolve_path(b_row, baseline_root, baseline_repo)
    c_path = resolve_path(c_row, current_root, current_repo)
    if not (b_path.exists() and c_path.exists()):
        add_issue(
            issues,
            category="dictionary_compare_skipped",
            severity="review",
            artifact=key,
            relative_path=DICTIONARY_REL,
            detail="Baseline or current dictionary file is not available on disk.",
        )
        return
    b_map, b_missing = dictionary_mapping(b_path)
    c_map, c_missing = dictionary_mapping(c_path)
    if b_missing or c_missing:
        add_issue(
            issues,
            category="dictionary_compare_skipped",
            severity="review",
            artifact=key,
            relative_path=DICTIONARY_REL,
            detail=f"Missing dictionary columns. baseline={','.join(b_missing)} current={','.join(c_missing)}",
        )
        return
    for mapping_key in sorted(set(b_map) - set(c_map)):
        add_issue(
            issues,
            category="dictionary_mapping_removed",
            severity="fail",
            artifact=key,
            relative_path=DICTIONARY_REL,
            field="|".join(mapping_key),
            baseline=b_map[mapping_key],
            current="",
        )
    for mapping_key in sorted(set(c_map) - set(b_map)):
        add_issue(
            issues,
            category="dictionary_mapping_added",
            severity="fail",
            artifact=key,
            relative_path=DICTIONARY_REL,
            field="|".join(mapping_key),
            baseline="",
            current=c_map[mapping_key],
        )
    for mapping_key in sorted(set(b_map) & set(c_map)):
        if b_map[mapping_key] != c_map[mapping_key]:
            add_issue(
                issues,
                category="dictionary_mapping_changed",
                severity="fail",
                artifact=key,
                relative_path=DICTIONARY_REL,
                field="|".join(mapping_key),
                baseline=b_map[mapping_key],
                current=c_map[mapping_key],
            )


def table_mapping(path: Path, key_cols: list[str]) -> tuple[dict[tuple[str, ...], str], list[str]]:
    df = pd.read_csv(path, dtype=str).fillna("")
    missing = [col for col in key_cols if col not in df.columns]
    if missing:
        return {}, missing
    value_cols = [col for col in df.columns if col not in key_cols]
    for col in key_cols + value_cols:
        df[col] = df[col].astype(str).str.strip()
    mapping = {
        tuple(norm(row[col]) for col in key_cols): " ; ".join(f"{col}={norm(row[col])}" for col in value_cols)
        for _, row in df.iterrows()
    }
    return mapping, []


def compare_csv_table(
    label: str,
    relative_path: str,
    key_cols: list[str],
    baseline_manifest: pd.DataFrame,
    current_manifest: pd.DataFrame,
    baseline_root: Path | None,
    current_root: Path | None,
    baseline_repo: Path | None,
    current_repo: Path | None,
    issues: list[dict[str, object]],
) -> None:
    b_row = artifact_row(baseline_manifest, relative_path)
    c_row = artifact_row(current_manifest, relative_path)
    if b_row is None and c_row is None:
        return
    if b_row is None or c_row is None:
        row = c_row if c_row is not None else b_row
        add_issue(
            issues,
            category=f"{label}_table_presence_changed",
            severity="fail",
            artifact=artifact_key(row),
            relative_path=relative_path,
            baseline=bool(b_row is not None),
            current=bool(c_row is not None),
        )
        return
    b_path = resolve_path(b_row, baseline_root, baseline_repo)
    c_path = resolve_path(c_row, current_root, current_repo)
    key = artifact_key(c_row)
    if not (b_path.exists() and c_path.exists()):
        add_issue(
            issues,
            category=f"{label}_compare_skipped",
            severity="review",
            artifact=key,
            relative_path=relative_path,
            detail="Baseline or current CSV file is not available on disk.",
        )
        return
    b_map, b_missing = table_mapping(b_path, key_cols)
    c_map, c_missing = table_mapping(c_path, key_cols)
    if b_missing or c_missing:
        add_issue(
            issues,
            category=f"{label}_compare_skipped",
            severity="review",
            artifact=key,
            relative_path=relative_path,
            detail=f"Missing key columns. baseline={','.join(b_missing)} current={','.join(c_missing)}",
        )
        return
    for row_key in sorted(set(b_map) - set(c_map)):
        add_issue(
            issues,
            category=f"{label}_row_removed",
            severity="fail",
            artifact=key,
            relative_path=relative_path,
            field="|".join(row_key),
            baseline=b_map[row_key],
            current="",
        )
    for row_key in sorted(set(c_map) - set(b_map)):
        add_issue(
            issues,
            category=f"{label}_row_added",
            severity="fail",
            artifact=key,
            relative_path=relative_path,
            field="|".join(row_key),
            baseline="",
            current=c_map[row_key],
        )
    for row_key in sorted(set(b_map) & set(c_map)):
        if b_map[row_key] != c_map[row_key]:
            add_issue(
                issues,
                category=f"{label}_changed",
                severity="fail",
                artifact=key,
                relative_path=relative_path,
                field="|".join(row_key),
                baseline=b_map[row_key],
                current=c_map[row_key],
            )


def render_markdown(summary: pd.DataFrame, issues: pd.DataFrame) -> str:
    total = int(len(issues))
    failing = int((issues["severity"] == "fail").sum()) if total else 0
    review = int((issues["severity"] == "review").sum()) if total else 0
    lines = [
        "# Release comparison",
        "",
        f"- total_findings: `{total}`",
        f"- failing_findings: `{failing}`",
        f"- review_findings: `{review}`",
        "",
    ]
    if not summary.empty:
        lines.append("## Summary")
        lines.append("")
        for row in summary.to_dict("records"):
            lines.append(f"- `{row['category']}` `{row['severity']}`: {row['findings']}")
        lines.append("")
    if total:
        lines.append("## First findings")
        lines.append("")
        for row in issues.head(50).to_dict("records"):
            lines.append(f"- `{row['severity']}` `{row['category']}` `{row['relative_path']}` {row['field']}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    current_root = Path(args.current_root).expanduser() if args.current_root else None
    baseline_root = Path(args.baseline_root).expanduser() if args.baseline_root else None
    current_repo = Path(args.current_repo_root).expanduser() if args.current_repo_root else None
    baseline_repo = Path(args.baseline_repo_root).expanduser() if args.baseline_repo_root else None
    current_manifest_path = (
        Path(args.current_manifest).expanduser()
        if args.current_manifest
        else data_layout(current_root).checks / "release_manifest" / "release_manifest.csv"
    )
    baseline_manifest_path = Path(args.baseline_manifest).expanduser()
    out_dir = Path(args.out_dir).expanduser() if args.out_dir else data_layout(current_root).checks / "release_compare"
    out_dir.mkdir(parents=True, exist_ok=True)

    baseline_manifest = read_csv(baseline_manifest_path)
    current_manifest = read_csv(current_manifest_path)
    overrides = load_overrides(Path(args.overrides).expanduser())
    required = {"path_base", "relative_path", "present", "role"}
    for label, df in [("baseline", baseline_manifest), ("current", current_manifest)]:
        missing = required - set(df.columns)
        if missing:
            raise SystemExit(f"{label} manifest missing columns: {', '.join(sorted(missing))}")

    issues: list[dict[str, object]] = []
    compare_manifest(baseline_manifest, current_manifest, issues)
    compare_parquet_files(baseline_manifest, current_manifest, baseline_root, current_root, baseline_repo, current_repo, issues)
    compare_dictionary(baseline_manifest, current_manifest, baseline_root, current_root, baseline_repo, current_repo, issues)
    for rel, keys in PRCH_TABLES.items():
        compare_csv_table("prch", rel, keys, baseline_manifest, current_manifest, baseline_root, current_root, baseline_repo, current_repo, issues)
    apply_overrides(issues, overrides)

    issue_df = pd.DataFrame(
        issues,
        columns=["category", "severity", "artifact_key", "relative_path", "field", "baseline", "current", "detail"],
    )
    issue_df.to_csv(out_dir / "release_comparison.csv", index=False)
    if issue_df.empty:
        summary = pd.DataFrame(columns=["category", "severity", "findings"])
    else:
        summary = issue_df.groupby(["category", "severity"], as_index=False).size().rename(columns={"size": "findings"})
    summary.to_csv(out_dir / "release_comparison_summary.csv", index=False)
    (out_dir / "release_comparison.md").write_text(render_markdown(summary, issue_df), encoding="utf-8")

    fail_threshold = FAIL_RANK[args.fail_on]
    max_rank = max((FAIL_RANK.get(sev, 0) for sev in issue_df["severity"]), default=0)
    if fail_threshold and max_rank >= fail_threshold:
        print(f"Release comparison failed: {len(issue_df)} findings written to {out_dir / 'release_comparison.csv'}")
        return 1
    print(f"Release comparison complete: {len(issue_df)} findings written to {out_dir / 'release_comparison.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
