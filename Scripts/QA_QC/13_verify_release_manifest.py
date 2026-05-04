#!/usr/bin/env python3
"""
QA 13: verify a release manifest against files on disk.

Reads:
- `Checks/release_manifest/release_manifest.csv`

Writes:
- `release_manifest_verification.csv`
- `release_manifest_verification.md`

The verifier is strict by default: rows recorded as missing fail verification,
and rows recorded as present must match size, SHA-256, and parquet shape.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, compute_file_metadata, repo_root


def parse_args() -> argparse.Namespace:
    data_root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True, help="release_manifest.csv")
    p.add_argument("--root", default=str(data_root), help="External IPEDSDB_ROOT")
    p.add_argument("--repo-root", default=str(repo_root()), help="Repository root")
    p.add_argument("--out-dir", default=None, help="Verification output dir. Defaults to manifest parent.")
    p.add_argument("--require-present", action=argparse.BooleanOptionalAction, default=True)
    return p.parse_args()


def boolish(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def parquet_shape(path: Path) -> tuple[str, str]:
    if not path.exists() or path.suffix.lower() != ".parquet":
        return "", ""
    meta = pq.ParquetFile(path).metadata
    return str(int(meta.num_rows)), str(int(meta.num_columns))


def resolve_path(row: pd.Series, root: Path, repo: Path) -> Path:
    path_base = str(row.get("path_base", "")).strip()
    rel = str(row.get("relative_path", "")).strip()
    if path_base == "IPEDSDB_ROOT":
        return root / rel
    if path_base == "repo":
        return repo / rel
    absolute = str(row.get("absolute_path", "")).strip()
    return Path(absolute) if absolute else Path(rel)


def verify_row(row: pd.Series, root: Path, repo: Path, require_present: bool) -> dict[str, object]:
    expected_present = boolish(row.get("present", ""))
    path = resolve_path(row, root, repo)
    actual_present = path.exists() and path.is_file()
    failures: list[str] = []
    if require_present and not expected_present:
        failures.append("manifest_recorded_missing")
    if expected_present and not actual_present:
        failures.append("missing_file")

    actual_size = ""
    actual_sha = ""
    actual_rows = ""
    actual_columns = ""
    if actual_present:
        actual_size, actual_sha = compute_file_metadata(path)
        actual_rows, actual_columns = parquet_shape(path)
        if expected_present and str(row.get("size_bytes", "")).strip() != actual_size:
            failures.append("size_mismatch")
        if expected_present and str(row.get("sha256", "")).strip() != actual_sha:
            failures.append("sha256_mismatch")
        expected_rows = str(row.get("rows", "")).strip()
        expected_columns = str(row.get("columns", "")).strip()
        if expected_rows and expected_rows != actual_rows:
            failures.append("parquet_rows_mismatch")
        if expected_columns and expected_columns != actual_columns:
            failures.append("parquet_columns_mismatch")

    return {
        "artifact_id": row.get("artifact_id", ""),
        "role": row.get("role", ""),
        "path_base": row.get("path_base", ""),
        "relative_path": row.get("relative_path", ""),
        "expected_present": expected_present,
        "actual_present": actual_present,
        "expected_size_bytes": row.get("size_bytes", ""),
        "actual_size_bytes": actual_size,
        "expected_sha256": row.get("sha256", ""),
        "actual_sha256": actual_sha,
        "expected_rows": row.get("rows", ""),
        "actual_rows": actual_rows,
        "expected_columns": row.get("columns", ""),
        "actual_columns": actual_columns,
        "passed": not failures,
        "failure_reasons": ",".join(failures),
    }


def render_markdown(results: pd.DataFrame) -> str:
    passed = int(results["passed"].astype(bool).sum()) if "passed" in results else 0
    total = int(len(results))
    failed = total - passed
    lines = [
        "# Release Manifest Verification",
        "",
        f"- checks_passed: `{passed}`",
        f"- checks_failed: `{failed}`",
        f"- artifact_rows: `{total}`",
        "",
    ]
    if failed:
        lines.append("## Failures")
        lines.append("")
        for row in results.loc[~results["passed"].astype(bool)].head(50).to_dict("records"):
            lines.append(f"- `{row['relative_path']}`: {row['failure_reasons']}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest).expanduser()
    root = Path(args.root).expanduser()
    repo = Path(args.repo_root).expanduser()
    out_dir = Path(args.out_dir).expanduser() if args.out_dir else manifest_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_csv(manifest_path, dtype=str).fillna("")
    required = {"artifact_id", "path_base", "relative_path", "present", "size_bytes", "sha256"}
    missing = required - set(manifest.columns)
    if missing:
        raise SystemExit(f"Manifest missing required columns: {', '.join(sorted(missing))}")

    results = pd.DataFrame(
        [verify_row(row, root, repo, bool(args.require_present)) for _, row in manifest.iterrows()]
    )
    results.to_csv(out_dir / "release_manifest_verification.csv", index=False)
    (out_dir / "release_manifest_verification.md").write_text(render_markdown(results), encoding="utf-8")

    failed = int((~results["passed"].astype(bool)).sum())
    if failed:
        print(f"Release manifest verification failed: {failed} artifact rows failed.")
        return 1
    print(f"Release manifest verification passed: {len(results)} artifact rows checked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
