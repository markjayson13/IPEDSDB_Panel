#!/usr/bin/env python3
"""
QA 12: build a release manifest with checksums and lightweight file metadata.

Reads:
- generated `IPEDSDB_ROOT` artifacts
- tracked release contracts and method docs in the repo

Writes:
- `Checks/release_manifest/release_manifest.csv`
- `Checks/release_manifest/release_manifest.json`
- `Checks/release_manifest/release_manifest_summary.csv`
- `Checks/release_manifest/release_manifest_summary.md`

This script creates the checksum ledger for a citable data release. It does not
decide whether a build is release-ready; run the acceptance audit first.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, compute_file_metadata, ensure_data_layout, parse_years, repo_root


MANIFEST_VERSION = "1.0"
QA_DIRS = [
    "download_qc",
    "extract_qc",
    "dictionary_qc",
    "harmonize_qc",
    "wide_qc",
    "disc_qc",
    "prch_qc",
    "panel_qc",
    "acceptance_qc",
    "release_metrics",
]
REPO_RELEASE_FILES = [
    "LICENSE",
    "DATA_LICENSE.md",
    "CONTACT.md",
    "ACKNOWLEDGMENTS.md",
    "CITATION.cff",
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "GOVERNANCE.md",
    "CHANGELOG.md",
    "METHODS_PANEL_CONSTRUCTION.md",
    "METHODS_PRCH_CLEANING.md",
    "codemeta.json",
    ".zenodo.json",
    ".github/CODEOWNERS",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".github/workflows/ci.yml",
    "requirements.txt",
    "contracts/panel_spec.toml",
    "contracts/dictionary_ambiguity_overrides.csv",
    "contracts/release_diff_overrides.csv",
    "contracts/known_limitations.csv",
    "Artifacts/release_validation_plan.md",
    "Artifacts/table_release_validation_metrics_template.csv",
    "Artifacts/legacy_analysis_schema_seed.csv",
    "docs/releases.html",
]


def parse_args() -> argparse.Namespace:
    data_root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(data_root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023", help='Release year span, e.g. "2004:2023"')
    p.add_argument("--out-dir", default=None, help="Output directory. Defaults to Checks/release_manifest under root.")
    p.add_argument("--include-qa-files", action=argparse.BooleanOptionalAction, default=True)
    return p.parse_args()


def git_value(args: list[str], cwd: Path) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=cwd, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def git_metadata(cwd: Path) -> dict[str, str]:
    status = git_value(["status", "--porcelain"], cwd)
    return {
        "git_commit": git_value(["rev-parse", "HEAD"], cwd),
        "git_branch": git_value(["branch", "--show-current"], cwd),
        "git_dirty": "true" if status else "false",
    }


def parquet_shape(path: Path) -> tuple[str, str]:
    if not path.exists() or path.suffix.lower() != ".parquet":
        return "", ""
    meta = pq.ParquetFile(path).metadata
    return str(int(meta.num_rows)), str(int(meta.num_columns))


def rel_or_blank(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def artifact_id(role: str, path_base: str, relative_path: str) -> str:
    safe = relative_path.replace("/", "__").replace(" ", "_")
    return f"{role}:{path_base}:{safe}"


def build_artifact_row(
    *,
    role: str,
    path_base: str,
    base_dir: Path,
    relative_path: str,
    years_spec: str,
    generated_at_utc: str,
    git_meta: dict[str, str],
    notes: str = "",
) -> dict[str, object]:
    path = base_dir / relative_path
    present = path.exists() and path.is_file()
    size_bytes, sha256 = compute_file_metadata(path)
    rows, columns = parquet_shape(path)
    return {
        "manifest_version": MANIFEST_VERSION,
        "generated_at_utc": generated_at_utc,
        "artifact_id": artifact_id(role, path_base, relative_path),
        "role": role,
        "path_base": path_base,
        "relative_path": relative_path,
        "absolute_path": str(path),
        "present": bool(present),
        "size_bytes": size_bytes,
        "sha256": sha256,
        "rows": rows,
        "columns": columns,
        "file_format": path.suffix.lower().lstrip("."),
        "years": years_spec,
        "git_commit": git_meta.get("git_commit", ""),
        "git_branch": git_meta.get("git_branch", ""),
        "git_dirty": git_meta.get("git_dirty", ""),
        "notes": notes,
    }


def discover_files(root: Path, rel_dir: str) -> Iterable[str]:
    base = root / rel_dir
    if not base.exists():
        return []
    return sorted(str(path.relative_to(root)) for path in base.rglob("*") if path.is_file())


def expected_root_artifacts(root: Path, years: list[int], include_qa_files: bool) -> list[tuple[str, str, str]]:
    start, end = years[0], years[-1]
    artifacts: list[tuple[str, str, str]] = [
        ("dictionary_artifact", "Dictionary/dictionary_lake.parquet", "Stitched variable metadata"),
        ("dictionary_artifact", "Dictionary/dictionary_codes.parquet", "Dictionary code labels"),
        ("panel_output", f"Panels/{start}-{end}/panel_long_varnum_{start}_{end}.parquet", "Stitched long panel"),
        ("panel_output", "Panels/panel_long_scalar_unique.parquet", "Scalar long lane"),
        ("panel_output", "Panels/panel_long_dim.parquet", "Dimensioned long lane"),
        ("panel_output", f"Panels/panel_wide_analysis_{start}_{end}.parquet", "Wide analysis panel"),
        ("panel_output", f"Panels/panel_clean_analysis_{start}_{end}.parquet", "PRCH-cleaned analysis panel"),
    ]
    for year in years:
        artifacts.append(("raw_manifest", f"Raw_Access_Databases/{year}/manifest.csv", "Year-level download/extract manifest"))
        artifacts.extend(("raw_download", rel, "Downloaded NCES source artifact") for rel in discover_files(root, f"Raw_Access_Databases/{year}/downloads"))
        artifacts.extend(("extract_metadata", rel, "Extracted Access metadata") for rel in discover_files(root, f"Raw_Access_Databases/{year}/metadata"))
        artifacts.append(("long_year_output", f"Cross_sections/panel_long_varnum_{year}.parquet", "Per-year harmonized long output"))
    if include_qa_files:
        for qa_dir in QA_DIRS:
            artifacts.extend(("qa_artifact", rel, f"QA artifact from {qa_dir}") for rel in discover_files(root, f"Checks/{qa_dir}"))
    return artifacts


def expected_repo_artifacts() -> list[tuple[str, str, str]]:
    return [("repo_contract", rel, "Tracked release contract or method document") for rel in REPO_RELEASE_FILES]


def render_summary(summary: dict[str, object]) -> str:
    lines = [
        "# Release Manifest Summary",
        "",
        f"- generated_at_utc: `{summary['generated_at_utc']}`",
        f"- years: `{summary['years']}`",
        f"- git_commit: `{summary['git_commit']}`",
        f"- git_dirty: `{summary['git_dirty']}`",
        f"- artifact_rows: `{summary['artifact_rows']}`",
        f"- present_artifacts: `{summary['present_artifacts']}`",
        f"- missing_artifacts: `{summary['missing_artifacts']}`",
        f"- total_present_bytes: `{summary['total_present_bytes']}`",
        "",
        "Run `Scripts/QA_QC/13_verify_release_manifest.py` against `release_manifest.csv` before archiving.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    layout = ensure_data_layout(args.root)
    years = parse_years(args.years)
    years_spec = f"{years[0]}:{years[-1]}"
    out_dir = Path(args.out_dir).expanduser() if args.out_dir else layout.checks / "release_manifest"
    out_dir.mkdir(parents=True, exist_ok=True)
    repo = repo_root()
    git_meta = git_metadata(repo)
    generated_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    rows: list[dict[str, object]] = []
    for role, rel, notes in expected_root_artifacts(layout.root, years, bool(args.include_qa_files)):
        rows.append(
            build_artifact_row(
                role=role,
                path_base="IPEDSDB_ROOT",
                base_dir=layout.root,
                relative_path=rel,
                years_spec=years_spec,
                generated_at_utc=generated_at_utc,
                git_meta=git_meta,
                notes=notes,
            )
        )
    for role, rel, notes in expected_repo_artifacts():
        rows.append(
            build_artifact_row(
                role=role,
                path_base="repo",
                base_dir=repo,
                relative_path=rel,
                years_spec=years_spec,
                generated_at_utc=generated_at_utc,
                git_meta=git_meta,
                notes=notes,
            )
        )

    manifest = pd.DataFrame(rows).sort_values(["role", "path_base", "relative_path"], kind="mergesort")
    manifest_csv = out_dir / "release_manifest.csv"
    manifest_json = out_dir / "release_manifest.json"
    manifest.to_csv(manifest_csv, index=False)
    manifest_json.write_text(json.dumps(manifest.to_dict("records"), indent=2), encoding="utf-8")

    present = manifest["present"].astype(bool)
    size_numeric = pd.to_numeric(manifest.loc[present, "size_bytes"], errors="coerce").fillna(0)
    summary = {
        "generated_at_utc": generated_at_utc,
        "years": years_spec,
        "git_commit": git_meta.get("git_commit", ""),
        "git_branch": git_meta.get("git_branch", ""),
        "git_dirty": git_meta.get("git_dirty", ""),
        "artifact_rows": int(len(manifest)),
        "present_artifacts": int(present.sum()),
        "missing_artifacts": int((~present).sum()),
        "total_present_bytes": int(size_numeric.sum()),
        "manifest_csv": str(manifest_csv),
        "manifest_json": str(manifest_json),
    }
    pd.DataFrame([summary]).to_csv(out_dir / "release_manifest_summary.csv", index=False)
    (out_dir / "release_manifest_summary.md").write_text(render_summary(summary), encoding="utf-8")

    print(f"Wrote release manifest to {manifest_csv}")
    if summary["missing_artifacts"]:
        print(f"[warn] release manifest contains missing artifacts: {summary['missing_artifacts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
