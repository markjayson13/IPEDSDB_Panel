#!/usr/bin/env python3
"""Build and publish the separate 2024 extension; never replace Final.

Run phases in order in a fresh work directory. Sources come from
prepare_2024_sources.py. Every publication is checksum-bound and uses one
directory rename followed by one Provisional symlink replacement.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from panel_extension import append_year, combine_metadata, compact_lineage, sha256, union_schema
from prepare_2024_pipeline import prepare, validate_sources
from prepare_2024_sources import verify_snapshot
from entity_quarantine import DEFAULT_POLICY, quarantine_mission_records, verify_quarantine

REPOSITORY = Path(__file__).resolve().parents[1]
PANEL = "panel_clean_prch_2004_2024"
BASE = "panel_clean_prch_2004_2023"
RELEASE = "2024-extension-v1"
CONSOLIDATION_POLICY = REPOSITORY / "contracts/harmonization/source-family-consolidation-v1.json"
STATUS = "Mixed final/provisional: see each 2024 source table's release status and date"
QUARANTINE_NOTE = ("The analysis dataset quarantines two unverified mission-only records: UNITID 111111 in 2019 and 2024. "
                   "Supporting/quarantined_mission_records.parquet retains both complete rows. Every other historical "
                   "observation and value is unchanged; Final and the original source data are unchanged.")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def run(argv: list[str], receipt: Path, *, env: dict | None = None) -> None:
    receipt.parent.mkdir(parents=True, exist_ok=True)
    record = {"command": argv, "started_utc": datetime.now(timezone.utc).isoformat()}
    write_json(receipt, record)
    with receipt.with_suffix(".log").open("w") as log:
        result = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                                env={**os.environ, **(env or {})})
    record.update(exit_code=result.returncode, completed_utc=datetime.now(timezone.utc).isoformat())
    write_json(receipt, record)
    if result.returncode:
        raise RuntimeError(f"Command failed; inspect {receipt.with_suffix('.log')}")


def artifacts(root: Path) -> list[dict]:
    result = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"Release packages cannot contain symlinks: {path}")
        if path.is_file() and path != root / "manifest.json":
            result.append({"path": str(path.relative_to(root)), "bytes": path.stat().st_size, "sha256": sha256(path)})
    return result


def verify_artifacts(root: Path, manifest: dict) -> None:
    expected = manifest["artifacts"]
    actual = artifacts(root)
    if actual != expected:
        raise ValueError("Release artifacts differ from their verified manifest")


def copy_quarantine_evidence(package: Path) -> None:
    """Retain the reviewed audit receipts in every fresh candidate reproduction."""
    rules = json.loads(DEFAULT_POLICY.read_text())
    for row in rules["records"]:
        evidence = row["evidence"]
        relative = Path(evidence["audit_receipt_name"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe mission source audit path")
        source = DEFAULT_POLICY.parent / "evidence" / relative
        target = package / "Checks/2024" / relative
        expected = evidence["audit_receipt_sha256"]
        if sha256(source) != expected:
            raise ValueError("Versioned mission source audit differs from the reviewed policy")
        if target.exists():
            if sha256(target) != expected:
                raise ValueError("Existing mission source audit differs from the reviewed policy")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if sha256(target) != expected:
            raise ValueError("Copied mission source audit differs from the reviewed policy")


def analysis_input(package: Path, *, verify_exports: bool = False) -> Path:
    """Require the authorized exact two-record exclusion and its retained-cell proof."""
    policy = package / "Reproduction/extension-code/contracts/source_quality/mission-orphans-v1.json"
    if sha256(policy) != sha256(DEFAULT_POLICY):
        raise ValueError("Packaged mission quarantine policy differs from the reviewed policy")
    rules = json.loads(policy.read_text())
    for row in rules["records"]:
        evidence = row["evidence"]
        audit = package / "Checks/2024" / evidence["audit_receipt_name"]
        if sha256(audit) != evidence["audit_receipt_sha256"]:
            raise ValueError("Mission quarantine source audit changed")
    raw = package / f"Supporting/{PANEL}.unannotated.parquet"
    selected = package / f"Supporting/{PANEL}.analysis.parquet"
    receipt = selected.with_name(selected.name + ".quarantine-validation.json")
    proof = verify_quarantine(raw, selected, package / "Supporting/quarantined_mission_records.parquet", receipt, policy)
    if verify_exports:
        for fmt in ("parquet", "dta"):
            metadata = json.loads((package / f"{PANEL}.{fmt}.metadata.json").read_text())
            if metadata.get("analysis_provenance") != proof["analysis_provenance"]:
                raise ValueError("Export omitted or changed its mission quarantine provenance")
    return selected


def verify_stata_history(package: Path, root: Path) -> dict:
    """Check published category numbers independently of the exporter's write path."""
    reference = root / f"Final/{BASE}.dta.metadata.json"
    old = json.loads(reference.read_text())
    current = json.loads((package / f"{PANEL}.dta.metadata.json").read_text())
    reference_hash = sha256(reference)
    if current.get("stata_encoding_reference", {}).get("sha256") != reference_hash:
        raise ValueError("Stata export is not bound to the historical encoding reference")
    variables = {row["name"]: row for row in current["variables"]}
    if len(variables) != len(current["variables"]):
        raise ValueError("Stata export repeats a variable identity")
    mappings = 0
    policy_path = package / "Reproduction/extension-code/contracts/harmonization/source-family-consolidation-v1.json"
    groups = json.loads(policy_path.read_text())["groups"] if policy_path.exists() else []
    renamed = {member["column"]: group["canonical_name"] for group in groups for member in group["members"]}
    consolidated = {}
    for before in old["variables"]:
        name = renamed.get(before["name"], before["name"])
        after = variables.get(name)
        expected_alias = name if before["name"] in renamed else before["export_name"]
        if (after is None or expected_alias != after["export_name"] or
                before.get("stata_storage_conversion", "none") != after.get("stata_storage_conversion", "none")):
            raise ValueError(f"Historical Stata variable or representation changed: {before['name']}")
        if name != before["name"]:
            consolidated[before["name"]] = name
        previous = {row["source_code"]: row["export_code"] for row in before.get("stata_source_code_map", [])}
        rows = after.get("stata_source_code_map", [])
        observed = {row["source_code"]: row["export_code"] for row in rows}
        if (len(observed) != len(rows) or len(set(observed.values())) != len(observed) or
                any(observed.get(token) != code for token, code in previous.items())):
            raise ValueError(f"Historical Stata category numbers changed: {before['name']}")
        if before.get("stata_storage_conversion") == "string_categories_to_numeric":
            if any(code <= max(previous.values()) for token, code in observed.items() if token not in previous):
                raise ValueError(f"New Stata categories must follow historical numbers: {before['name']}")
        mappings += len(previous)
    return {"status": "pass", "reference_sha256": reference_hash,
            "historical_variables_checked": len(old["variables"]), "historical_category_numbers_checked": mappings,
            "all_historical_aliases_conversions_and_category_numbers_equal": not consolidated,
            "all_historical_value_encodings_equal": True,
            "consolidated_aliases": consolidated,
            "unmerged_historical_aliases_unchanged": True}


def _consolidation_policy(package: Path) -> Path:
    """Bind both the reviewed registry and its compressed source evidence."""
    policy = package / "Reproduction/extension-code/contracts/harmonization/source-family-consolidation-v1.json"
    if sha256(policy) != sha256(CONSOLIDATION_POLICY):
        raise ValueError("Packaged consolidation policy differs from the reviewed registry")
    rule = json.loads(policy.read_text())
    relative = Path(rule.get("evidence_file", ""))
    if not relative.name or relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Consolidation policy has an unsafe or missing evidence path")
    evidence = policy.parent / relative
    if evidence.is_symlink() or sha256(evidence) != rule.get("evidence_sha256"):
        raise ValueError("Consolidation source evidence differs from the reviewed policy")
    return policy


def _verify_consolidated_identities(package: Path, policy: Path) -> dict:
    """Independently check every compact lineage field and exact column/year remap."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from panel_extension import identity, values_equal

    source = package / "Metadata/column_identities.parquet"
    output = package / "Metadata/consolidated_column_identities.parquet"
    receipt = output.with_name(output.name + ".consolidation-identities.json")
    checks_receipt = package / "Checks/consolidated_column_identities.json"
    paths = (source, output, policy, receipt, checks_receipt)
    snapshots = {path: identity(path) for path in paths}
    record = json.loads(receipt.read_text())
    hashes = {"source_sha256": sha256(source), "data_sha256": sha256(output), "policy_sha256": sha256(policy)}
    if (record.get("status") != "pass" or any(record.get(key) != value for key, value in hashes.items()) or
            json.loads(checks_receipt.read_text()) != record):
        raise ValueError("Consolidated lineage receipts are not bound to current source, output, and policy")
    before, after = pq.read_table(source), pq.read_table(output)
    if (not {"analysis_column", "year"}.issubset(before.column_names) or
            "source_panel_column" in before.column_names or
            len(set(before.column_names)) != len(before.column_names)):
        raise ValueError("Consolidated lineage requires unique original column/year identities")
    schema = before.schema.append(before.schema.field("analysis_column").with_name("source_panel_column"))
    if before.num_rows != after.num_rows or not after.schema.equals(schema, check_metadata=True):
        raise ValueError("Consolidated lineage changed original rows, schema, or column order")
    groups = json.loads(policy.read_text())["groups"]
    mapping = {(member["column"], year): group["canonical_name"] for group in groups
               for member in group["members"] for year in member["years"]}
    registered = {column for column, _ in mapping}
    keys = list(zip(before["analysis_column"].to_pylist(), before["year"].to_pylist()))
    if any(column in registered and (column, year) not in mapping for column, year in keys):
        raise ValueError("Consolidated lineage contains out-of-scope registered identities")
    expected_names = pa.array([mapping.get(key, key[0]) for key in keys], type=before["analysis_column"].type)
    observed_names = after["analysis_column"].combine_chunks()
    if (not expected_names.is_null().equals(observed_names.is_null()) or
            not values_equal(expected_names, observed_names)):
        raise ValueError("Consolidated lineage changed the approved column/year remap")
    for name in before.column_names:
        expected = before[name].combine_chunks()
        observed = after["source_panel_column" if name == "analysis_column" else name].combine_chunks()
        if not expected.is_null().equals(observed.is_null()) or not values_equal(expected, observed):
            raise ValueError(f"Consolidated lineage changed an original field: {name}")
    mapped = sum(key in mapping for key in keys)
    proof = {"status": "pass", **hashes, "records": before.num_rows, "mapped_records": mapped,
             "unchanged_records": before.num_rows - mapped, "all_original_fields_equal": True,
             "all_original_missingness_equal": True, "out_of_scope_registered_identities": []}
    if record != proof:
        raise ValueError("Consolidated lineage receipt lacks complete independent remapping proof")
    if any(identity(path) != previous for path, previous in snapshots.items()):
        raise ValueError("Consolidated lineage artifacts changed during verification")
    return proof


def consolidated_input(package: Path, *, verify_exports: bool = False) -> Path:
    """Require an exactly reversible, reviewed consolidation after quarantine."""
    from panel_consolidation import verify_consolidation

    original = analysis_input(package, verify_exports=verify_exports)
    policy = _consolidation_policy(package)
    selected = package / f"Supporting/{PANEL}.consolidated.parquet"
    proof = verify_consolidation(original, selected,
                                selected.with_name(selected.name + ".consolidation-validation.json"), policy)
    _verify_consolidated_identities(package, policy)
    if verify_exports:
        import pyarrow.parquet as pq

        groups = {group["canonical_name"]: group for group in proof["column_consolidation"]["groups"]}
        for fmt in ("parquet", "dta"):
            metadata = json.loads((package / f"{PANEL}.{fmt}.metadata.json").read_text())
            if metadata.get("column_consolidation") != proof["column_consolidation"]:
                raise ValueError("Export omitted or changed its column consolidation provenance")
            variables = metadata.get("variables", [])
            names = [variable["name"] for variable in variables]
            if (len(names) != len(set(names)) or not set(groups).issubset(names) or
                    any(variable.get("column_consolidation") != groups.get(variable["name"]) for variable in variables)):
                raise ValueError("Export variable consolidation crosswalk differs from the reviewed groups")
        embedded = (pq.read_schema(package / f"{PANEL}.parquet").metadata or {}).get(b"ipeds:column_consolidation")
        if embedded is None or json.loads(embedded) != proof["column_consolidation"]:
            raise ValueError("Parquet export omitted or changed its embedded column consolidation provenance")
    return selected


def consolidate(work: Path) -> None:
    """Consolidate verified source-family moves without rebuilding source years."""
    from panel_consolidation import consolidate as write_consolidated, consolidate_identities

    package = work / "package"
    if (package / "manifest.json").exists():
        raise ValueError("Do not modify a sealed package; prepare a fresh candidate checkpoint")
    original = analysis_input(package)
    frozen = package / "Reproduction/extension-code"
    for directory in ("Scripts", "contracts", "tests"):
        shutil.copytree(REPOSITORY / directory, frozen / directory, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"))
    (frozen / "docs").mkdir(exist_ok=True)
    shutil.copy2(REPOSITORY / "docs/2024_EXTENSION.md", frozen / "docs/2024_EXTENSION.md")
    policy = _consolidation_policy(package)
    output = package / f"Supporting/{PANEL}.consolidated.parquet"
    write_consolidated(original, output, policy)
    result = consolidate_identities(package / "Metadata/column_identities.parquet",
                                    package / "Metadata/consolidated_column_identities.parquet", policy)
    write_json(package / "Checks/consolidated_column_identities.json", result)
    _verify_consolidated_identities(package, policy)


def verify_append_evidence(package: Path, root: Path) -> None:
    """A later export cannot substitute a changed historical append."""
    import pyarrow.parquet as pq

    combined = package / f"Supporting/{PANEL}.unannotated.parquet"
    receipt = json.loads(combined.with_name(combined.name + ".append-validation.json").read_text())
    inputs = [root / f"Final/{BASE}.parquet", package / "Supporting/panel_clean_prch_2024_2024.parquet"]
    hashes = [sha256(path) for path in inputs]
    source_hashes = receipt.get("source_sha256", {})
    if (receipt.get("status") != "pass" or receipt.get("year") != 2024 or
            receipt.get("data_sha256") != sha256(combined) or len(source_hashes) != 2 or
            sorted(source_hashes.values()) != sorted(hashes)):
        raise ValueError("Historical append receipt is not bound to the current inputs and combined data")
    parity = receipt.get("parity", {})
    blocks = parity.get("blocks", [])
    if parity.get("status") != "pass" or len(blocks) != 2:
        raise ValueError("Historical append lacks complete parity evidence")
    readers = [pq.ParquetFile(path) for path in inputs]
    for block, reader, digest in zip(blocks, readers, hashes):
        if (source_hashes.get(block.get("source")) != digest or
                block.get("rows") != reader.metadata.num_rows or block.get("columns") != len(reader.schema_arrow) or
                any(block.get(key) is not True for key in
                    ("all_values_equal", "all_missingness_equal", "out_of_scope_columns_all_null"))):
            raise ValueError("Historical append has incomplete source-block parity evidence")
    actual = pq.ParquetFile(combined)
    expected_schema = union_schema(readers[0].schema_arrow, readers[1].schema_arrow)
    if (actual.metadata.num_rows != sum(reader.metadata.num_rows for reader in readers) or
            not actual.schema_arrow.equals(expected_schema, check_metadata=False)):
        raise ValueError("Historical append shape or schema changed")
    for name in ("dictionary_lake", "dictionary_codes"):
        record = json.loads((package / f"Checks/{name}_append.json").read_text())
        original = root / f"Final/Metadata/{name}.parquet"
        incoming = package / f"Metadata/2024/{name}.parquet"
        output = package / f"Metadata/{name}.parquet"
        if (record.get("historical_records_unchanged") is not True or record.get("new_records_unchanged") is not True or
                record.get("sha256") != sha256(output) or
                sorted(record.get("source_sha256", {}).values()) != sorted([sha256(original), sha256(incoming)])):
            raise ValueError(f"Metadata append evidence changed: {name}")


def verify_2024_evidence(package: Path) -> None:
    """Bind passing source and readiness checks to the actual packaged files."""
    from export_integrity import require_export_ready
    import pyarrow.parquet as pq

    receipt = json.loads((package / "Checks/raw_source_validation.json").read_text())
    if (receipt.get("status") != "passed" or receipt.get("year") != 2024 or
            receipt.get("raw_to_wide_discrepancies") != 0 or receipt.get("unexpected_cleaning_changes") != 0):
        raise ValueError("2024 independent source validation did not pass")
    locations = {
        "wide": "Supporting/panel_wide_analysis_2024_2024.parquet",
        "clean": "Supporting/panel_clean_prch_2024_2024.parquet",
        "mapping": "Reproduction/pipeline-2024/contracts/extension_2024/variable_mapping.csv",
        "policy": "Reproduction/pipeline-2024/contracts/extension_2024/prch_policy.csv",
        "actions": "Checks/2024/v2/prch_qc/prch_cell_actions.parquet",
    }
    for key, relative in locations.items():
        if receipt.get("artifacts", {}).get(key, {}).get("sha256") != sha256(package / relative):
            raise ValueError(f"2024 source receipt is not bound to the packaged {key}")
    clean = pq.ParquetFile(package / locations["clean"])
    with (package / locations["mapping"]).open(newline="") as handle:
        mapping = list(csv.DictReader(handle))
    if (receipt.get("rows") != clean.metadata.num_rows or receipt.get("columns") != len(clean.schema_arrow)
            or receipt.get("scalar_variables") != len(mapping)
            or receipt.get("compared_panel_cells_including_missing") != len(mapping) * clean.metadata.num_rows):
        raise ValueError("2024 source validation has incomplete cell coverage")
    with (package / "Sources/2024/Raw_Access_Databases/2024/metadata/table_inventory.csv").open(newline="") as handle:
        inventory = {row["table_name"].upper(): row["csv_path"] for row in csv.DictReader(handle)}
    tables = receipt.get("scalar_source_tables", [])
    expected_tables = {row["access_table_name"].upper() for row in mapping}
    if len(tables) != len(expected_tables) or {row["table"].upper() for row in tables} != expected_tables:
        raise ValueError("2024 source validation omits a scalar source table")
    for row in tables:
        relative = Path(inventory[row["table"].upper()])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe source table path")
        if row["sha256"] != sha256(package / "Sources/2024/Raw_Access_Databases/2024" / relative):
            raise ValueError(f"2024 source table changed after validation: {row['table']}")
    metadata_path = package / "Checks/2024_metadata/panel.parquet.metadata.json"
    metadata = json.loads(metadata_path.read_text())
    require_export_ready(metadata)
    checked = pq.ParquetFile(package / "Checks/2024_metadata/panel.parquet")
    if (checked.metadata.num_rows != clean.metadata.num_rows or
            not checked.schema_arrow.equals(clean.schema_arrow, check_metadata=False) or
            metadata.get("row_count") != clean.metadata.num_rows or
            metadata.get("column_count") != len(clean.schema_arrow) or
            [row.get("name") for row in metadata.get("variables", [])] != clean.schema_arrow.names):
        raise ValueError("2024 strict metadata check does not cover the complete new-year panel")
    if (metadata.get("data_sha256") != sha256(package / "Checks/2024_metadata/panel.parquet")
            or metadata.get("source_panel_sha256") != receipt["artifacts"]["clean"]["sha256"]):
        raise ValueError("2024 strict metadata check is not bound to the packaged data")
    for key, relative in {"dictionary": "Metadata/2024/dictionary_lake.parquet",
                          "codes": "Metadata/2024/dictionary_codes.parquet",
                          "lineage": "Checks/2024/v2/wide_qc/qc_value_lineage.parquet"}.items():
        if metadata.get("metadata_source_sha256", {}).get(key) != sha256(package / relative):
            raise ValueError(f"2024 strict metadata source changed: {key}")


def baseline(root: Path) -> dict:
    path = root / "Final/manifest.json"
    manifest = json.loads(path.read_text())
    lookup = {row["path"]: row for row in manifest["artifacts"]}
    names = [f"Final/{BASE}.{ext}{suffix}" for ext in ("parquet", "dta") for suffix in ("", ".metadata.json")]
    names += ["Final/Metadata/dictionary_lake.parquet", "Final/Metadata/dictionary_codes.parquet", "Final/value_lineage.parquet"]
    rows = []
    for name in names:
        row = lookup[name]
        if sha256(root / name) != row["sha256"]:
            raise ValueError(f"Historical release artifact changed: {name}")
        rows.append(row)
    return {"manifest_sha256": sha256(path), "artifacts": rows}


def build(work: Path, sources: Path, spill_root: Path | None = None) -> None:
    verify_snapshot(sources)
    pipeline, data = work / "pipeline", work / "build"
    spill_root = spill_root or work / "duckdb-spill"
    if data.exists():
        raise ValueError("Build phase requires a fresh build directory")
    if not pipeline.exists():
        prepare(pipeline, REPOSITORY, sources)
    (data / "Raw_Access_Databases").mkdir(parents=True)
    (data / "Raw_Access_Databases/2024").symlink_to(sources.resolve() / "Raw_Access_Databases/2024", target_is_directory=True)
    panels, checks = data / "Panels/v2", data / "Checks/v2"
    dictionary = data / "Dictionary/v2/dictionary_lake.parquet"
    scalar = panels / "panel_long_scalar_2024_2024.parquet"
    dimensioned = panels / "panel_long_dimensioned_2024_2024.parquet"
    wide = panels / "panel_wide_analysis_2024_2024.parquet"
    clean = panels / "panel_clean_prch_2024_2024.parquet"
    common = ["--root", str(data), "--years", "2024"]
    commands = [
        ["03_dictionary_ingest.py", *common],
        ["04_harmonize.py", *common, "--chunksize", "10000", "--value-cols-per-chunk", "80", "--duckdb-memory-limit", "2GB", "--dedupe-threads", "2"],
        ["05_stitch_long.py", *common, "--duckdb-memory-limit", "4GB", "--duckdb-threads", "1",
         "--duckdb-temp-dir", str(spill_root / "stage05"), "--batch-rows", "10000", "--stitch-batch-rows", "100000"],
        ["06_build_wide_panel.py", "--root", str(data), "--years", "2024:2024", "--input", str(scalar), "--dimensioned-input", str(dimensioned),
         "--source-column-coverage", str(checks / "harmonize_qc"), "--out_dir", str(panels / "wide_by_year"),
         "--write_single", str(wide), "--dictionary", str(dictionary), "--qc-dir", str(checks / "wide_qc"),
         "--raw-wide-out", str(panels / "panel_wide_raw_2024_2024.parquet"),
         "--duckdb-memory-limit", "4GB", "--duckdb-threads", "2", "--scan-batch-rows", "10000",
         "--duckdb-path", str(data / "build/v2/ipeds_build.duckdb"),
         "--duckdb-temp-dir", str(spill_root / "stage06")],
        ["07_clean_panel.py", "--input", str(wide), "--output", str(clean), "--dictionary", str(dictionary),
         "--column-lineage", str(checks / "wide_qc/qc_value_lineage.parquet"),
         "--qc-dir", str(checks / "prch_qc"), "--batch-rows", "250"],
    ]
    for command in commands:
        run([sys.executable, str(pipeline / "Scripts" / command[0]), *command[1:]],
            data / f"Checks/logs/{command[0][:2]}_command.json", env={"IPEDSDB_ROOT": str(data)})


def assemble(work: Path, sources: Path, root: Path) -> None:
    verify_snapshot(sources)
    prepared = json.loads((work / "pipeline/extension_2024_overlay.json").read_text())
    if validate_sources(sources, REPOSITORY) != prepared["source_preflight"]:
        raise ValueError("Requested sources differ from the prepared pipeline's verified inputs")
    base = baseline(root)
    data, package = work / "build", work / "package"
    if package.exists():
        raise ValueError("Assemble requires a fresh package directory")
    package.mkdir(parents=True)
    write_json(package / "Checks/historical_release.json", base)
    old_panel, new_panel = root / f"Final/{BASE}.parquet", data / "Panels/v2/panel_clean_prch_2024_2024.parquet"
    wide = data / "Panels/v2/panel_wide_analysis_2024_2024.parquet"
    run([sys.executable, str(REPOSITORY / "Scripts/QA_QC/29_validate_2024_extension.py"),
         "--source-root", str(sources), "--wide", str(wide), "--clean", str(new_panel),
         "--mapping", str(work / "pipeline/contracts/extension_2024/variable_mapping.csv"),
         "--policy", str(work / "pipeline/contracts/extension_2024/prch_policy.csv"),
         "--actions", str(data / "Checks/v2/prch_qc/prch_cell_actions.parquet"),
         "--output", str(package / "Checks/raw_source_validation.json")], package / "Checks/raw_source_validation_command.json")
    run([sys.executable, str(REPOSITORY / "Scripts/08_build_custom_panel.py"), "--root", str(data),
         "--input", str(new_panel), "--output", str(package / "Checks/2024_metadata/panel.parquet"),
         "--all-vars", "--year-scoped-labels", "--require-metadata", "--batch-rows", "4096", "--log-file", "",
         "--dictionary", str(data / "Dictionary/v2/dictionary_lake.parquet"),
         "--codes", str(data / "Dictionary/v2/dictionary_codes.parquet"),
         "--column-lineage", str(data / "Checks/v2/wide_qc/qc_value_lineage.parquet")], package / "Checks/2024_metadata_command.json")
    old_hash = next(row["sha256"] for row in base["artifacts"] if row["path"] == f"Final/{BASE}.parquet")
    append_year(old_panel, new_panel, package / f"Supporting/{PANEL}.unannotated.parquet", expected_base_sha256=old_hash)
    quarantine_mission_records(package / f"Supporting/{PANEL}.unannotated.parquet",
                              package / f"Supporting/{PANEL}.analysis.parquet",
                              package / "Supporting/quarantined_mission_records.parquet")
    for name in ("dictionary_lake", "dictionary_codes"):
        result = combine_metadata(root / f"Final/Metadata/{name}.parquet", data / f"Dictionary/v2/{name}.parquet",
                                  package / f"Metadata/{name}.parquet")
        write_json(package / f"Checks/{name}_append.json", result)
    shutil.copytree(data / "Dictionary/v2", package / "Metadata/2024")
    lineage = data / "Checks/v2/wide_qc/qc_value_lineage.parquet"
    compact_lineage([root / "Final/value_lineage.parquet", lineage], package / "Metadata/column_identities.parquet")
    shutil.copytree(data / "Checks", package / "Checks/2024", symlinks=False)
    copy_quarantine_evidence(package)
    for name in ("panel_clean_prch", "panel_wide_analysis", "panel_long_scalar", "panel_long_dimensioned"):
        path = data / f"Panels/v2/{name}_2024_2024.parquet"
        shutil.copy2(path, package / "Supporting" / path.name)
    shutil.copytree(work / "pipeline", package / "Reproduction/pipeline-2024", ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache", ".git"))
    current_code = package / "Reproduction/extension-code"
    for directory in ("Scripts", "contracts", "tests"):
        shutil.copytree(REPOSITORY / directory, current_code / directory,
                        ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"))
    for path in REPOSITORY.glob("requirements*.txt"):
        shutil.copy2(path, current_code / path.name)
    (current_code / "docs").mkdir()
    shutil.copy2(REPOSITORY / "docs/2024_EXTENSION.md", current_code / "docs/2024_EXTENSION.md")
    shutil.copytree(sources, package / "Sources/2024")
    verify_snapshot(package / "Sources/2024")
    write_json(package / "Metadata/value_lineage_sources.json", {
        "historical": next(row for row in base["artifacts"] if row["path"] == "Final/value_lineage.parquet"),
        "new_year": {"path": "Checks/2024/v2/wide_qc/qc_value_lineage.parquet", "sha256": sha256(lineage)},
        "note": "Full raw-value lineage remains split by year range; column_identities.parquet is only the compact metadata lookup."})


def export(work: Path, root: Path) -> None:
    package = work / "package"
    raw = consolidated_input(package)
    common = [sys.executable, str(REPOSITORY / "Scripts/08_build_custom_panel.py"), "--root", str(work / "build"),
              "--input", str(raw), "--all-vars", "--year-scoped-labels", "--batch-rows", "4096", "--log-file", "",
              "--dictionary", str(package / "Metadata/dictionary_lake.parquet"),
              "--codes", str(package / "Metadata/dictionary_codes.parquet"),
              "--column-lineage", str(package / "Metadata/consolidated_column_identities.parquet")]
    for fmt in ("parquet", "dta"):
        output = package / f"{PANEL}.{fmt}"
        if output.exists():
            raise ValueError(f"Export already exists; inspect it before rebuilding: {output}")
        extra = ["--stata-reference-metadata", str(root / f"Final/{BASE}.dta.metadata.json")] if fmt == "dta" else []
        run([*common, "--output", str(output), *extra], package / f"Checks/export_{fmt}.json")


def validate(work: Path, root: Path) -> None:
    from build_codebook import build as codebook
    from codebook_pdf import render_pdf, write_manifest
    from publish_labeled_panel import verify_package

    package = work / "package"
    if (package / "manifest.json").exists():
        raise ValueError("Candidate already has a manifest; do not overwrite a validated package")
    verify_2024_evidence(package)
    verify_append_evidence(package, root)
    saved = json.loads((package / "Checks/historical_release.json").read_text())
    if baseline(root) != saved:
        raise ValueError("Historical release changed during extension")
    raw = consolidated_input(package, verify_exports=True)
    write_json(package / "Checks/stata_history_validation.json", verify_stata_history(package, root))
    if not (package / "Checks/native_stata/validation.json").exists():
        run([sys.executable, str(REPOSITORY / "Scripts/QA_QC/28_validate_full_stata.py"), "--input", str(package / f"{PANEL}.dta"),
             "--source-panel", str(raw), "--output-dir", str(package / "Checks/native_stata")], package / "Checks/native_stata_command.json")
    # A saved native checkpoint still passes all current hash, coverage, and log checks below.
    result, _ = verify_package(package, raw, allow_source_gaps=True, panel_name=f"{PANEL}.parquet")
    result["parquet_parity"].pop("source_names", None)
    result.pop("artifacts")
    write_json(package / "Checks/export_validation.json", result)
    manifest = {"schema_version": 1, "release_name": RELEASE, "status": "building", "source_release_status": STATUS,
                "created_utc": datetime.now(timezone.utc).isoformat(), "historical_release": saved,
                "main_panel": f"{PANEL}.parquet", "metadata_gaps": result["metadata_gaps"],
                "codebook_url": "https://markjayson13.github.io/IPEDSDB_Panel/provisional/",
                "analysis_selection": json.loads((package / f"Supporting/{PANEL}.analysis.parquet.quarantine-validation.json").read_text()),
                "column_consolidation": json.loads(raw.with_name(raw.name + ".consolidation-validation.json").read_text()),
                "codebook_notes": [
                    QUARANTINE_NOTE,
                    "Verified source-table moves are consolidated into canonical columns without recoding values. The downloadable column crosswalk records every consolidated source column and its years; Supporting retains the source-preserving analysis panel. Different or unresolved measures remain separate. Documented later definition and code changes still require year-specific interpretation.",
                    "Actual 2024 SFA imputation flags, including XUPGRNTN and XUPGRNTT, are retained in the release at Sources/2024/sidecars/sfa2324_supplement.csv with a matching .metadata.json codebook. Join that file by UNITID and year. These source flags are separate from the main panel.",
                    "Other source flag supplements are in Sources/2024/sidecars/. Use each supplement's complete documented keys; detailed tables may have multiple records per institution-year.",
                    "Collection year does not establish identical IPEDS and FSA populations or reference periods. For 2024 Pell, read the variable's reporting-population note before linking annual FSA volume."],
                "artifacts": artifacts(package)}
    codebook_input = package / "Checks/codebook_inputs.json"
    write_json(codebook_input, manifest)
    codebook(package, package / "Codebook", manifest_path=codebook_input, panel_relative=PANEL)
    render_pdf(package / "Codebook")
    write_manifest(package / "Codebook", json.loads((package / "Codebook/index.json").read_text()))
    (package / "README.md").write_text(
        f"# IPEDS 2004–2024 extension\n\nUse `{PANEL}.parquet` or `{PANEL}.dta`. This is a mixed final/provisional extension. "
        "The separate Final/ release remains 2004–2023.\n\n"
        f"{QUARANTINE_NOTE}\n\n"
        "The main dataset consolidates verified source-table moves into 95 canonical variables, reducing "
        "2,785 source columns to 2,676 analysis columns. Codebook/column-crosswalk.csv records every consolidated source "
        "column and its years. Supporting/panel_clean_prch_2004_2024.analysis.parquet retains the 2,785-column "
        "source-preserving version. Every original cell is recoverable exactly. Measures with different or "
        "unresolved definitions remain separate; later definition changes still require year-specific interpretation.\n\n"
        "Codebook/ contains the PDF and downloadable dictionaries. Metadata/ contains exact year-specific definitions, "
        "category codes, and lineage locations. Sources/2024/sidecars/ retains actual imputation and revision flags "
        "with their natural keys and labels; these fields are not invented in the main panel.\n\n"
        "Checks/ contains full append value/missingness equality and native Stata readback evidence. "
        "Supporting/ contains the 2024 raw, clean, scalar and dimensioned panels used for reproduction. "
        "Sources/ retains original Access and official CSV evidence; Reproduction/ retains the exact pipeline.\n\n"
        "Historical source metadata gaps remain disclosed. A matching collection year does not establish equal "
        "IPEDS and FSA reporting periods or student populations. See UPGRNTN/UPGRNTT source notes.\n")
    manifest.update(status="verified", artifacts=artifacts(package))
    write_json(package / "manifest.json", manifest)


def publish(package: Path, root: Path) -> dict:
    manifest_hash = sha256(package / "manifest.json")
    manifest = json.loads((package / "manifest.json").read_text())
    if sha256(package / "manifest.json") != manifest_hash:
        raise ValueError("Release manifest changed while being read")
    if manifest.get("status") != "verified" or manifest.get("release_name") != RELEASE:
        raise ValueError("Expected a verified extension package")
    verify_artifacts(package, manifest)
    verify_2024_evidence(package)
    verify_append_evidence(package, root)
    consolidated_input(package, verify_exports=True)
    if verify_stata_history(package, root) != json.loads((package / "Checks/stata_history_validation.json").read_text()):
        raise ValueError("Historical Stata encoding validation receipt changed")
    if baseline(root) != manifest["historical_release"]:
        raise ValueError("Historical release changed since extension validation")
    destination = root / "Releases" / RELEASE
    pointer = root / "Provisional"
    temporary = root / ".Provisional-pending"
    if destination.exists() or pointer.exists() or pointer.is_symlink():
        raise ValueError("Extension destination already exists; never silently replace a release")
    if temporary.exists() or temporary.is_symlink():
        raise ValueError("An unfinished Provisional pointer exists")
    (root / "Work").mkdir(exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".2024-publish-", dir=root / "Work"))
    promoted = False
    try:
        shutil.copytree(package, staging, dirs_exist_ok=True)
        verify_artifacts(staging, manifest)
        if sha256(staging / "manifest.json") != manifest_hash or sha256(package / "manifest.json") != manifest_hash:
            raise ValueError("Release manifest changed during publication")
        if baseline(root) != manifest["historical_release"]:
            raise ValueError("Historical release changed during publication")
        staging.rename(destination)
        promoted = True
        temporary.symlink_to(Path("Releases") / RELEASE, target_is_directory=True)
        os.replace(temporary, pointer)
    except Exception:
        if temporary.is_symlink():
            temporary.unlink()
        if promoted and not pointer.exists():
            shutil.rmtree(destination)
        raise
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {"status": "published", "path": str(pointer), "manifest_sha256": sha256(destination / "manifest.json")}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["candidate", "build", "assemble", "consolidate", "export", "validate", "publish"], required=True,
                        help="candidate runs build, assemble, consolidate, export and validation in one command")
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--sources", type=Path)
    parser.add_argument("--spill-root", type=Path, help="DuckDB temporary storage; defaults to WORK/duckdb-spill. Use a drive with ample free space.")
    parser.add_argument("--root", type=Path, default=Path("/Volumes/CIRAGO/IPEDSDB_PANEL"))
    args = parser.parse_args()
    work, root = args.work.resolve(), args.root.resolve()
    sources = (args.sources or work / "sources").resolve()
    if args.phase == "candidate":
        build(work, sources, args.spill_root)
        assemble(work, sources, root)
        consolidate(work)
        export(work, root)
        validate(work, root)
    elif args.phase == "build": build(work, sources, args.spill_root)
    elif args.phase == "assemble": assemble(work, sources, root)
    elif args.phase == "consolidate": consolidate(work)
    elif args.phase == "export": export(work, root)
    elif args.phase == "validate": validate(work, root)
    else: print(json.dumps(publish(work / "package", root), indent=2))


if __name__ == "__main__":
    main()
