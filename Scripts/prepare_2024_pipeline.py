#!/usr/bin/env python3
"""Prepare the preserved v2 pipeline for a locked, single-year 2024 extension.

Historical data are not rebuilt. The extension contracts contain only exact
2024 table/column decisions; the original PRCH policy bytes remain the prefix
of the extended policy. Source preflight rejects changed/unknown columns and
violations of each reviewed table key before any pipeline stage is run.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

import pandas as pd

CONTRACT_ID = "ipedsdb-panel-2004-2024-mixed-v1"
BASE_CONTRACT_ID = "ipedsdb-panel-2004-2023-access-v2"
EXTENSION = Path("contracts/extension_2024")


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read_rows(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def validate_sources(source_root: Path, repository: Path) -> dict:
    """Bind every physical field and verify every complete source-table key."""
    year_dir = source_root / "Raw_Access_Databases/2024"
    inventory = read_rows(year_dir / "metadata/table_inventory.csv")
    tables = {row["table_name"].upper(): row for row in inventory
              if row["table_role"] == "data" and row["has_unitid"].lower() in {"true", "1"}}
    grains = read_rows(repository / EXTENSION / "table_grain.csv")
    columns = read_rows(repository / EXTENSION / "source_columns.csv")
    expected_tables = {row["access_table_pattern"] for row in grains}
    if set(tables) != expected_tables:
        raise ValueError(f"2024 physical table inventory changed: extra={sorted(set(tables)-expected_tables)} "
                         f"missing={sorted(expected_tables-set(tables))}")
    evidence = []
    for grain in grains:
        table = grain["access_table_pattern"]
        path = year_dir / tables[table]["csv_path"]
        header = pd.read_csv(path, nrows=0).columns.str.upper().str.strip().tolist()
        expected = [row["column_name"] for row in columns if row["access_table_pattern"] == table]
        if len(set(header)) != len(header) or set(header) != set(expected):
            raise ValueError(f"2024 physical columns changed in {table}: "
                             f"extra={sorted(set(header)-set(expected))} missing={sorted(set(expected)-set(header))}")
        keys = grain["unit_key_columns"].split("|")
        data = pd.read_csv(path, dtype=str, keep_default_na=False,
                           usecols=lambda column: column.upper().strip() in keys)
        data.columns = data.columns.str.upper().str.strip()
        for key in keys:
            data[key] = data[key].str.strip()
        if data[keys].eq("").any().any():
            raise ValueError(f"2024 missing source key in {table}: {keys}")
        # Exact duplicate source rows may be collapsed by Stage04; conflicting
        # payloads at an otherwise identical key are never silently discarded.
        duplicate_rows = int(data.duplicated(keys).sum())
        if duplicate_rows:
            payload = pd.read_csv(path, dtype=str, keep_default_na=False)
            payload.columns = header
            for key in keys:
                payload[key] = payload[key].str.strip()
            if payload.drop_duplicates().duplicated(keys).any():
                raise ValueError(f"2024 conflicting duplicate source key in {table}: {keys}")
        if str(len(data)) != tables[table]["row_count_csv"]:
            raise ValueError(f"2024 source row count differs from inventory for {table}")
        evidence.append({"table": table, "rows": len(data), "columns": len(header),
                         "key": keys, "exact_duplicate_rows": duplicate_rows, "sha256": digest(path)})
    return {"contract_id": CONTRACT_ID, "tables": evidence,
            "inventory_sha256": digest(year_dir / "metadata/table_inventory.csv")}


def prepare(output: Path, repository: Path, source_root: Path | None = None) -> dict:
    repository = repository.resolve()
    if output.exists():
        raise ValueError("2024 pipeline output must be a new directory")
    module_path = repository / "Scripts/QA_QC/25_prepare_v2_repair_pipeline.py"
    spec = importlib.util.spec_from_file_location("prepare_v2_repair", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    source_checks = validate_sources(source_root, repository) if source_root else None
    source_manifest = source_root / "source_manifest.json" if source_root else None
    if source_manifest is not None and not source_manifest.is_file():
        raise ValueError("2024 preparation requires a locked source_manifest.json")
    receipt = module.prepare(output, repository)
    patch = repository / EXTENSION / "v2-extension.patch"
    subprocess.run(["git", "apply", str(patch)], cwd=output, check=True)
    # This is a new contract. The archived base itself is never rewritten.
    # Compatibility registries keep their decisions, rebinding only contract ID.
    for path in [output / "Scripts/panel_v2_schema.py", *sorted((output / "contracts").glob("*.csv"))]:
        if path.name == "prch_policy.csv":
            continue
        path.write_text(path.read_text().replace(BASE_CONTRACT_ID, CONTRACT_ID))
    spec_path = output / "contracts/panel_spec.toml"
    value = spec_path.read_text().replace(BASE_CONTRACT_ID, CONTRACT_ID)
    value = value.replace('end_year = 2023', 'end_year = 2024')
    value = value.replace('final_only = true', 'final_only = false').replace('include_provisional = false', 'include_provisional = true')
    value = value.replace('default_year_spec = "2004:2023"', 'default_year_spec = "2024"')
    spec_path.write_text(value)
    for name in ["table_grain.csv", "source_columns.csv", "analysis_schema.csv", "discrete_families.csv"]:
        shutil.copy2(repository / EXTENSION / name, output / "contracts" / name)
    policy = output / "contracts/prch_policy.csv"
    base_policy = policy.read_bytes()
    with (repository / EXTENSION / "prch_policy.csv").open("rb") as handle:
        handle.readline()  # Preserve original header and every historical byte.
        policy.write_bytes(base_policy + handle.read())
    if hashlib.sha256(base_policy).hexdigest() != module.POLICY_SHA256:
        raise ValueError("Historical PRCH policy changed")
    for name in ["extension_2024_metadata.py", "extension_2024_null_columns.py"]:
        shutil.copy2(repository / "Scripts" / name, output / "Scripts" / name)
    shutil.copytree(repository / EXTENSION, output / EXTENSION)
    receipt.update(contract_id=CONTRACT_ID, extension_patch_sha256=digest(patch),
                   historical_prch_prefix_bytes=len(base_policy), source_preflight=source_checks,
                   extension_contracts=[{"path": str(p.relative_to(repository)), "sha256": digest(p)}
                                        for p in sorted((repository / EXTENSION).glob("*")) if p.is_file()])
    receipt["base_pipeline_scripts"] = receipt["pipeline_scripts"]
    receipt["pipeline_scripts"] = [{"path": str(path.relative_to(output)), "sha256": digest(path)}
                                   for path in sorted((output / "Scripts").rglob("*.py"))]
    if source_manifest:
        receipt["source_manifest"] = {"path": str(source_manifest.resolve()), "sha256": digest(source_manifest)}
    (output / "extension_2024_overlay.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-root", type=Path)
    args = parser.parse_args()
    result = prepare(args.output, args.repository, args.source_root)
    print(json.dumps({"contract_id": result["contract_id"], "output": str(args.output)}))
