#!/usr/bin/env python3
"""
QA 16: build Frictionless-style Data Package metadata for a release.

Reads:
- `Checks/release_manifest/release_manifest.csv`
- present CSV and parquet artifacts listed in the manifest

Writes:
- `Checks/release_metadata/datapackage.json`
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import DEFAULT_IPEDSDB_ROOT, data_layout, parse_years, repo_root


def parse_args() -> argparse.Namespace:
    root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023")
    p.add_argument("--manifest", default=None)
    p.add_argument("--out", default=None)
    p.add_argument("--repo-root", default=str(repo_root()))
    p.add_argument("--name", default="ipedsdb_panel")
    p.add_argument("--title", default="IPEDSDB_Panel")
    p.add_argument("--version", default="ipedsdb-panel-2004-2023-access-final-v1")
    p.add_argument("--homepage", default="https://github.com/markjayson13/IPEDSDB_Panel")
    return p.parse_args()


def boolish(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def resource_name(relative_path: str) -> str:
    return (
        relative_path.lower()
        .replace("/", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace(" ", "_")
    )


def resolve_path(row: pd.Series, root: Path, repo: Path) -> Path:
    base = str(row.get("path_base", "")).strip()
    rel = str(row.get("relative_path", "")).strip()
    if base == "IPEDSDB_ROOT":
        return root / rel
    if base == "repo":
        return repo / rel
    absolute = str(row.get("absolute_path", "")).strip()
    return Path(absolute) if absolute else Path(rel)


def parquet_fields(path: Path) -> list[dict[str, str]]:
    fields = []
    schema = pq.ParquetFile(path).schema_arrow
    for field in schema:
        fields.append({"name": field.name, "type": str(field.type)})
    return fields


def csv_fields(path: Path) -> list[dict[str, str]]:
    try:
        columns = list(pd.read_csv(path, nrows=0).columns)
    except Exception:
        return []
    return [{"name": str(col), "type": "string"} for col in columns]


def fields_for(path: Path) -> list[dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return parquet_fields(path)
    if suffix == ".csv":
        return csv_fields(path)
    return []


def primary_key(relative_path: str, fields: list[dict[str, str]]) -> list[str]:
    names = {field["name"] for field in fields}
    if relative_path.startswith("Panels/") and {"UNITID", "year"}.issubset(names):
        return ["UNITID", "year"]
    if "release_manifest.csv" in relative_path and "artifact_id" in names:
        return ["artifact_id"]
    return []


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser()
    repo = Path(args.repo_root).expanduser()
    layout = data_layout(root)
    years = parse_years(args.years)
    manifest_path = Path(args.manifest).expanduser() if args.manifest else layout.checks / "release_manifest" / "release_manifest.csv"
    out_path = Path(args.out).expanduser() if args.out else layout.checks / "release_metadata" / "datapackage.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_csv(manifest_path, dtype=str).fillna("")
    resources = []
    for _, row in manifest.iterrows():
        if not boolish(row.get("present", "")):
            continue
        rel = str(row.get("relative_path", "")).strip()
        path = resolve_path(row, root, repo)
        if not path.exists() or not path.is_file():
            continue
        fmt = str(row.get("file_format", "")).strip().lower()
        fields = fields_for(path)
        schema = {"fields": fields}
        pkey = primary_key(rel, fields)
        if pkey:
            schema["primaryKey"] = pkey
        resources.append(
            {
                "name": resource_name(rel),
                "path": rel,
                "title": rel,
                "profile": "tabular-data-resource" if fields else "data-resource",
                "format": fmt,
                "bytes": int(row["size_bytes"]) if str(row.get("size_bytes", "")).isdigit() else None,
                "hash": row.get("sha256", ""),
                "schema": schema if fields else {},
            }
        )

    package = {
        "profile": "data-package",
        "name": args.name,
        "title": args.title,
        "version": args.version,
        "homepage": args.homepage,
        "created": pd.Timestamp.utcnow().replace(microsecond=0).isoformat(),
        "description": f"IPEDSDB_Panel release metadata for {years[0]}:{years[-1]}.",
        "contributors": [{"title": "Mark Jayson Farol", "role": "author"}],
        "resources": sorted(resources, key=lambda item: item["path"]),
    }
    out_path.write_text(json.dumps(package, indent=2), encoding="utf-8")
    print(f"Wrote Data Package metadata to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
