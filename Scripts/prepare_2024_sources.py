#!/usr/bin/env python3
"""Freeze a 2024 Access snapshot plus explicitly checked current CSV overlays.

Original Access metadata and exports remain under ``original/``. The root's
Raw_Access_Databases layout is the effective Stage 03/04 input. Extra CSV item
status fields are preserved as source sidecars, never silently added to the
panel. A completed snapshot cannot be overwritten; reruns verify every hash.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import openpyxl
import pandas as pd


ACCESS_NAME = "IPEDS_2024-25_Provisional.zip"
ACCESS_SHA = "ef134955ba5003a07d37e46fae6286a76dca4c15767c3c4cd9086b1887896713"
DATABASE_SHA = "98e9175e54ba719cf7f0bfcc8e043fac4d11babd1b73ba1701ebbdc23e5470f5"
BASE_URL = "https://nces.ed.gov/ipeds/complete-data-files/"
FALL_TABLES = ("IC2024", "C2024_A", "C2024_B", "C2024_C", "C2024DEP", "DRVC2024", "DRVEF122024", "EFFY2024", "EFFY2024_DIST", "EFFY2024_HS", "EFIA2024")
OVERLAY_TABLES = ("HD2024", "FLAGS2024", *FALL_TABLES, "SFA2324")
KEYS = {"C2024_A": ["UNITID", "CIPCODE", "MAJORNUM", "AWLEVEL"], "C2024_C": ["UNITID", "AWLEVELC"], "C2024DEP": ["UNITID", "CIPCODE"], "EFFY2024": ["UNITID", "EFFYALEV", "EFFYLEV", "LSTUDY"], "EFFY2024_DIST": ["UNITID", "EFFYDLEV"]}

# Separately audited natural-key changes in locked official final members. They
# preserve the documented grain and do not change the historical panel.
ROW_REVISIONS = {
    "C2024_A": (175, 57, "2a18c4f335630fdfc6c53c2290b69b096e32ef314c211d3b980aa8dedb8b78a0"),
    "C2024_C": (5, 1, "8817a1135f67831dd1119302d9bc3610d8275231640a837ee8c839bcf2076208"),
    "C2024DEP": (72, 17, "fb0cc167eee5e5e08a20cab1ebaf7bc76214be8078e88f3b713d45bb74b1beaf"),
    "EFFY2024": (13, 2, "f1ae6c99f2a50cd77d03b6b6b36b32466da890fc9617969c41a7aea8402a0f95"),
    "EFFY2024_DIST": (4, 0, "8db78b9e8d6dfeb7ef3ff68bebe2dd452dd4e666709a8de9dc97acf61178301f"),
    "EFFY2024_HS": (1, 0, "25e9541fd20ced8002dae3475a50159aba05b761fc1c18f606550ba599e0cc1a"),
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    names = [str(c).strip().upper() for c in df.columns]
    if len(set(names)) != len(names):
        raise ValueError(f"Duplicate normalized columns: {path}")
    df.columns = names
    return df.apply(lambda c: c.str.strip())


def canonical(value: str, numeric: bool):
    text = value.strip()
    if not text:
        return None
    if numeric:
        try:
            result = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"Non-numeric source value {text!r}") from exc
        if not result.is_finite():
            raise ValueError(f"Non-finite source value {text!r}")
        return result
    return text


def indexed(df: pd.DataFrame, keys: list[str], numeric: set[str]) -> pd.DataFrame:
    result = df.copy()
    for key in keys:
        if key not in result:
            raise ValueError(f"Missing grain key {key}")
        result[key] = result[key].map(lambda x: canonical(x, key in numeric))
        if result[key].isna().any():
            raise ValueError(f"Missing grain key values: {key}")
    if result.duplicated(keys).any():
        raise ValueError(f"Duplicate source grain: {keys}")
    return result.set_index(keys).sort_index()


def compare_sources(access: pd.DataFrame, original: pd.DataFrame, selected: pd.DataFrame,
                    keys: list[str], numeric: set[str], expected_row_changes: tuple[int, int] | None = None) -> dict:
    """Require full coverage and equal grain; report revisions without guessing types."""
    for name, other in [("original", original), ("selected", selected)]:
        absent = sorted(set(access.columns) - set(other.columns))
        if absent:
            raise ValueError(f"{name} CSV omits Access columns: {absent}")
    a, o, s = [indexed(df, keys, numeric) for df in [access, original, selected]]
    if not a.index.equals(o.index):
        raise ValueError("Access and original CSV key sets differ")
    added = s.index.difference(a.index)
    removed = a.index.difference(s.index)
    if (len(added), len(removed)) != (expected_row_changes or (0, 0)):
        raise ValueError("Source key sets differ; review additions/removals explicitly")
    common = a.index.intersection(s.index)
    changes, baseline = {}, {}
    for col in a.columns:
        # Compare Python values directly: pandas string inference can turn None
        # into NaN, which would count matching blank strings as changes.
        n0 = sum(canonical(x, col in numeric) != canonical(y, col in numeric)
                 for x, y in zip(a[col], o[col]))
        n1 = sum(canonical(x, col in numeric) != canonical(y, col in numeric)
                 for x, y in zip(a.loc[common, col], s.loc[common, col]))
        if n0:
            baseline[col] = n0
        if n1:
            changes[col] = n1
    return {"original_rows": len(access), "effective_rows": len(selected), "common_rows": len(common),
            "added_rows": len(added), "removed_rows": len(removed), "grain": keys, "original_columns": list(access.columns),
            "extra_columns": sorted(set(selected.columns) - set(access.columns)),
            "access_vs_original_changed_cells": sum(baseline.values()),
            "access_vs_original_changes": baseline,
            "access_vs_effective_changed_cells": sum(changes.values()),
            "access_vs_effective_changes": changes}


def extract_zip(path: Path, destination: Path) -> list[dict]:
    destination.mkdir(parents=True, exist_ok=True)
    result = []
    with zipfile.ZipFile(path) as archive:
        bad = archive.testzip()
        if bad:
            raise ValueError(f"ZIP CRC failure: {path}: {bad}")
        for info in archive.infolist():
            relative = Path(info.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"Unsafe archive member: {info.filename}")
            if info.is_dir():
                continue
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(info))
            result.append({"member": info.filename, "sha256": sha256(target), "bytes": target.stat().st_size})
    return result


def workbook_records(path: Path) -> dict[str, list[dict]]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    result = {}
    for sheet in wb:
        if sheet.title == "Introduction":
            result[sheet.title] = [{"text": " ".join(str(x) for x in row if x is not None)} for row in sheet.values]
            continue
        rows = iter(sheet.values)
        header = [str(x or "").strip() for x in next(rows)]
        result[sheet.title] = [{k: "" if v is None else str(v) for k, v in zip(header, row) if k} for row in rows if any(x is not None for x in row)]
    wb.close()
    return result


def dictionary_info(records: dict) -> tuple[set[str], dict[str, str], dict[str, str]]:
    numeric, flags, labels = set(), {}, {}
    for row in records["Varlist"]:
        name = row["varName"].strip().upper()
        if row["DataType"].strip().upper() == "N":
            numeric.add(name)
        flag = row.get("imputationvar", "").strip().upper()
        if flag:
            if flag in flags and flags[flag] != name:
                raise ValueError(f"Ambiguous imputation association: {flag}")
            flags[flag] = name
    for row in records.get("Imputation values", []):
        code, label = row["CodeValue"].strip(), row["ValueLabel"].strip()
        if code in labels and labels[code] != label:
            raise ValueError(f"Conflicting imputation code label: {code}")
        labels[code] = label
    return numeric, flags, labels


def stata_item_status_labels(text: str) -> dict[str, str]:
    """Read only NCES's explicit item-status comment block, never infer codes."""
    labels = {}
    active = False
    for line in text.splitlines():
        if line.strip() == "*The following are the possible values for the item imputation field variables":
            active = True
            continue
        if not active:
            continue
        match = re.fullmatch(r"\*([A-Z])\s+(.+)", line.strip())
        if not match:
            break
        code, label = match.groups()
        if code in labels and labels[code] != label:
            raise ValueError(f"Conflicting source import-script status label: {code}")
        labels[code] = label
    return labels


def copy_archive(name: str, cache: Path, target: Path, download: bool) -> dict:
    source = cache / name
    if not source.exists():
        if not download:
            raise FileNotFoundError(f"Download required: {BASE_URL}{name}; use --download")
        source.parent.mkdir(parents=True, exist_ok=True)
        temp = source.with_suffix(".download")
        subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--silent", "--show-error", BASE_URL + name, "--output", str(temp)], check=True)
        with zipfile.ZipFile(temp) as archive:
            if archive.testzip():
                raise ValueError(f"Invalid archive: {name}")
        temp.replace(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return {"url": BASE_URL + name, "archive_sha256": sha256(target), "archive_bytes": target.stat().st_size}


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def verify_snapshot(root: Path) -> dict:
    manifest = json.loads((root / "source_manifest.json").read_text())
    for item in manifest["artifacts"]:
        p = root / item["path"]
        if not p.is_file() or sha256(p) != item["sha256"]:
            raise ValueError(f"Frozen source artifact changed or missing: {p}")
    return manifest


def build_snapshot(root: Path, audit: Path, cache: Path, download: bool, reuse_verified_snapshot: Path | None = None) -> dict:
    if sha256(audit / ACCESS_NAME) != ACCESS_SHA:
        raise ValueError("Audited Access archive hash changed")
    original = root / "original"
    original_year = original / "Raw_Access_Databases/2024"
    downloads = original_year / "downloads"
    release = next(r for r in json.loads((audit / "release_inventory.json").read_text()) if r["year"] == 2024)
    if reuse_verified_snapshot is not None:
        verify_snapshot(reuse_verified_snapshot)
        shutil.copytree(reuse_verified_snapshot / "original", original)
    else:
        downloads.mkdir(parents=True)
        shutil.copyfile(audit / ACCESS_NAME, downloads / ACCESS_NAME)
        pd.DataFrame([release]).to_csv(original_year / "manifest.csv", index=False)
        script_dir = Path(__file__).resolve().parent
        subprocess.run([sys.executable, str(script_dir / "02_extract_access_db.py"), "--root", str(original), "--years", "2024"], check=True)
    database = next((original_year / "extracted_db").rglob("*.accdb"))
    if sha256(database) != DATABASE_SHA:
        raise ValueError("Extracted database hash differs from original verified database")
    year_dir = root / "Raw_Access_Databases/2024"
    year_dir.mkdir(parents=True)
    for directory in ["tables_csv", "metadata", "qc"]:
        shutil.copytree(original_year / directory, year_dir / directory)
    shutil.copyfile(original_year / "manifest.csv", year_dir / "manifest.csv")
    for directory in ["downloads", "extracted_db"]:
        (year_dir / directory).symlink_to(Path("../../original/Raw_Access_Databases/2024") / directory, target_is_directory=True)
    inventory = pd.read_csv(year_dir / "metadata/table_inventory.csv", dtype=str, keep_default_na=False)
    table_meta = read_csv(year_dir / "tables_csv/tables24.csv").set_index("TABLENAME")
    source_definitions = read_csv(year_dir / "tables_csv/vartable24.csv")
    metadata_differences = []
    tables = []
    for row in inventory.to_dict("records"):
        table = row["table_name"]
        meta = table_meta.loc[table.upper()].to_dict() if table.upper() in table_meta.index else {}
        tables.append({"table": table, "effective_csv": str((year_dir / row["csv_path"]).relative_to(root)), "original_csv_sha256": row["csv_sha256"],
                       "origin": "original_access", "release_status": meta.get("RELEASE", "Access metadata"), "release_date": meta.get("RELEASE_DATE", "March 2026"),
                       "reference_period": meta.get("YEARCOVERAGE", ""), "archive_url": release["access_url"], "archive_sha256": ACCESS_SHA,
                       "database_sha256": DATABASE_SHA})
    lookup = {r["table"].upper(): r for r in tables}
    for table in OVERLAY_TABLES:
        print(f"Preparing checked overlay {table}", flush=True)
        overlay = root / "overlays" / table
        data_zip = overlay / (table + ".zip")
        dictionary_zip = overlay / (table + "_Dict.zip")
        archive_info = copy_archive(data_zip.name, cache, data_zip, download)
        dictionary_archive = copy_archive(dictionary_zip.name, cache, dictionary_zip, download)
        data_members = extract_zip(data_zip, overlay / "data")
        dictionary_members = extract_zip(dictionary_zip, overlay / "dictionary")
        csv_members = {Path(i["member"]).name.lower(): i for i in data_members if i["member"].lower().endswith(".csv")}
        original_name = table.lower() + ".csv"
        revised_name = table.lower() + "_rv.csv"
        if original_name not in csv_members:
            raise ValueError(f"Missing exact original table CSV: {table}")
        if table in FALL_TABLES and revised_name not in csv_members:
            raise ValueError(f"Final revised fall member missing: {table}")
        selected_name = revised_name if revised_name in csv_members else original_name
        selected_member = csv_members[selected_name]
        workbooks = [i for i in dictionary_members if i["member"].lower().endswith(".xlsx")]
        if len(workbooks) != 1:
            raise ValueError(f"Expected exactly one dictionary workbook: {table}")
        records = workbook_records(overlay / "dictionary" / workbooks[0]["member"])
        write_json(overlay / "dictionary.json", records)
        numeric, associations, labels = dictionary_info(records)
        for definition in records["Varlist"]:
            name = definition["varName"].strip().upper()
            matches = source_definitions[(source_definitions["TABLENAME"].str.upper() == table) & (source_definitions["VARNAME"].str.upper() == name)]
            if matches.empty:
                continue
            if len(matches) != 1:
                raise ValueError(f"Ambiguous exact table/name definition: {table}.{name}")
            for access_field, csv_field in [("VARNUMBER", "varNumber"), ("DATATYPE", "DataType"), ("IMPUTATIONVAR", "imputationvar"), ("VARTITLE", "varTitle")]:
                old = str(matches.iloc[0][access_field]).strip()
                new = str(definition.get(csv_field) or "").strip()
                if old != new:
                    difference = {"table": table, "variable": name, "field": access_field, "original_access": old, "current_csv_dictionary": new}
                    if access_field != "VARTITLE":
                        raise ValueError(f"Source definition identity changed: {difference}")
                    metadata_differences.append(difference)
        effective_path = root / lookup[table]["effective_csv"]
        access = read_csv(effective_path)
        original_csv = read_csv(overlay / "data" / csv_members[original_name]["member"])
        selected = read_csv(overlay / "data" / selected_member["member"])
        keys = KEYS.get(table, ["UNITID"])
        allowed = ROW_REVISIONS.get(table)
        if allowed and archive_info["archive_sha256"] != allowed[2]:
            raise ValueError(f"Reviewed revised row-key archive changed: {table}")
        retained_exception = table == "C2024_A"
        compared_access = access
        coverage_exception = None
        if retained_exception:
            original_keys = indexed(original_csv, keys, numeric).index
            access_indexed = indexed(access, keys, numeric)
            if len(original_keys.difference(access_indexed.index)):
                raise ValueError("C_A original CSV keys are not a subset of original Access")
            extra = access_indexed.loc[access_indexed.index.difference(original_keys)]
            if len(access) != 1688471 or len(extra) != 1380764:
                raise ValueError("Reviewed C_A expanded Access coverage changed")
            compared_access = access_indexed.loc[original_keys].reset_index()
            # Only numeric grain fields are canonicalized; CIP identifiers remain text.
            for key in keys:
                compared_access[key] = compared_access[key].map(str)
            coverage_exception = {
                "classification": "non_equivalent_source_row_coverage",
                "access_rows": len(access), "original_standalone_rows": len(original_csv),
                "access_only_rows": len(extra),
                "access_only_cip_string_lengths": {str(k): int(v) for k, v in extra.reset_index()["CIPCODE"].str.len().value_counts().items()},
                "access_only_nonzero_total_rows": int(extra["CTOTALT"].map(lambda v: canonical(v, True) != 0).sum()),
                "policy": "Retain complete original provisional Access C_A in dimensioned lane. Final revised standalone CSV retained separately. Do not mix its details with provisional Access aggregates or synthesize absent rows. No C_A measures enter institution-year scalar panel.",
            }
        checks = compare_sources(compared_access, original_csv, selected, keys, numeric, allowed[:2] if allowed else None)
        if coverage_exception:
            checks["coverage_exception"] = coverage_exception
        if allowed:
            a = indexed(compared_access, keys, numeric)
            s = indexed(selected, keys, numeric)
            changes = []
            for action, ids in [("added", s.index.difference(a.index)), ("removed", a.index.difference(s.index))]:
                for key in ids:
                    values = key if isinstance(key, tuple) else (key,)
                    changes.append({**dict(zip(keys, map(str, values))), "action": action,
                                    "original_csv_sha256": lookup[table]["original_csv_sha256"],
                                    "revised_member_sha256": selected_member["sha256"]})
            ledger = overlay / "row_changes.csv"
            pd.DataFrame(changes).to_csv(ledger, index=False)
            checks["row_change_ledger"] = str(ledger.relative_to(root))
            checks["revision_reason"] = "Explicit natural-key additions/removals in official final revised member; original grain retained and keys validated unique/nonmissing."
        defined = {r["varName"].strip().upper() for r in records["Varlist"]}
        undefined = sorted(set(access.columns) - defined)
        if undefined:
            raise ValueError(f"Access columns absent from exact current dictionary: {table}: {undefined}")
        if table in FALL_TABLES and checks["access_vs_original_changed_cells"]:
            raise ValueError(f"Access versus original CSV differs unexpectedly: {table}: {checks['access_vs_original_changes']}")
        if table == "SFA2324" and checks["access_vs_effective_changed_cells"]:
            raise ValueError("SFA CSV values no longer match verified Access source")
        # Every extra field must have an exact workbook definition or item-status association.
        variable_definitions = {r["varName"].strip().upper(): r for r in records["Varlist"]}
        unexpected = sorted(set(checks["extra_columns"]) - set(associations) - set(variable_definitions))
        if unexpected:
            raise ValueError(f"Extra columns without exact dictionary definition: {table}: {unexpected}")
        sidecar = None
        status_supplements = []
        import_info = None
        if checks["extra_columns"]:
            import_zip = overlay / (table + "_stata.zip")
            import_info = copy_archive(import_zip.name, cache, import_zip, download)
            import_members = extract_zip(import_zip, overlay / "import_dictionary")
            do_files = [r for r in import_members if r["member"].lower().endswith(".do")]
            if len(do_files) != 1:
                raise ValueError(f"Expected one official Stata import dictionary: {table}")
            import_info["members"] = import_members
            import_labels = stata_item_status_labels((overlay / "import_dictionary" / do_files[0]["member"]).read_text(encoding="utf-8-sig"))
            fields = checks["extra_columns"]
            field_labels = {}
            unresolved_codes = []
            for name in fields:
                if name in associations:
                    field_labels[name] = dict(labels)
                    for code in sorted(set(selected[name]) - set(labels) - {""}):
                        if code in import_labels:
                            field_labels[name][code] = import_labels[code]
                            status_supplements.append({"variable": name, "code": code, "label": import_labels[code],
                                                       "reason": "Observed code absent from Excel dictionary; meaning explicitly documented in exact-table NCES Stata import script.",
                                                       "source_url": import_info["url"], "archive_sha256": import_info["archive_sha256"],
                                                       "member": do_files[0]["member"], "member_sha256": do_files[0]["sha256"]})
                else:
                    frequencies = records.get("FrequenciesRV", records.get("Frequencies", []))
                    field_labels[name] = {r["CodeValue"].strip(): r["valuelabel"].strip() for r in frequencies if r["VarName"].strip().upper() == name}
                unknown = set(selected[name]) - set(field_labels[name]) - {""}
                for code in sorted(unknown):
                    unresolved_codes.append({"variable": name, "code": code, "observations": int(selected[name].eq(code).sum()),
                                             "reason": "Observed source code absent from exact current dictionary; preserved without an inferred label."})
            sidecar_path = root / "sidecars" / (table.lower() + "_supplement.csv")
            sidecar_path.parent.mkdir(parents=True, exist_ok=True)
            side = selected[keys + fields].copy()
            side.insert(len(keys), "year", "2024")
            side.to_csv(sidecar_path, index=False)
            sidecar = {"path": str(sidecar_path.relative_to(root)), "grain": keys + ["year"], "supplementary_variables": fields,
                       "value_variable_associations": {k: associations[k] for k in fields if k in associations},
                       "variable_definitions": {k: variable_definitions[k] for k in fields if k in variable_definitions},
                       "value_labels_by_variable": field_labels,
                       "unresolved_codes": unresolved_codes,
                       "verified_code_supplements": status_supplements,
                       "metadata_complete": not unresolved_codes,
                       "meaning": "Actual source item/revision statuses; no inference from reported amounts", "sha256": sha256(sidecar_path)}
            write_json(sidecar_path.with_suffix(".metadata.json"), sidecar)
        # Retain canonical Access column names/order; dtype=str preserves identifier zeros.
        if not retained_exception:
            selected[access.columns].to_csv(effective_path, index=False)
        effective_sha = sha256(effective_path)
        mask = inventory["table_name"].str.upper() == table
        inventory.loc[mask, "csv_sha256"] = effective_sha
        inventory.loc[mask, "export_status"] = "original_access_explicit_coverage_exception" if retained_exception else "verified_csv_overlay"
        inventory.loc[mask, "row_count_csv"] = str(len(access) if retained_exception else len(selected))
        status = "Final" if table in FALL_TABLES else ("Provisional/final (not revisable)" if table == "HD2024" else ("Mixed: final fall / provisional winter and spring" if table == "FLAGS2024" else "Provisional"))
        date = "2026-09-08" if table in FALL_TABLES or table == "FLAGS2024" else ("2025-12-09" if table == "SFA2324" else "September 2025")
        revision_info = {"origin": "official_csv_overlay", "release_status": status, "release_date": date,
                              "workbook_release_text": records["Introduction"][:10], "data_archive": archive_info,
                              "data_member": selected_member, "dictionary_archive": dictionary_archive,
                              "dictionary_members": dictionary_members, "dictionary_json": str((overlay / "dictionary.json").relative_to(root)),
                              "effective_csv_sha256": effective_sha, "checks": checks, "item_status_sidecar": sidecar, "import_dictionary": import_info}
        if retained_exception:
            revision_info.pop("effective_csv_sha256")
            revision_info["applied_to_effective"] = False
            lookup[table]["unapplied_revision"] = revision_info
            lookup[table]["coverage_exception"] = coverage_exception
        else:
            lookup[table].update(revision_info)
        write_json(overlay / "verification.json", lookup[table])
    for row in tables:
        row["effective_csv_sha256"] = sha256(root / row["effective_csv"])
        mask = inventory["table_name"].str.upper() == row["table"].upper()
        for field, value in {
            "source_release_type": row["release_status"],
            "source_release_date": row["release_date"],
            "source_url": row.get("data_archive", {}).get("url", row["archive_url"]),
            "source_archive_sha256": row.get("data_archive", {}).get("archive_sha256", ACCESS_SHA),
            "source_member_sha256": row.get("data_member", {}).get("sha256", row["original_csv_sha256"]),
        }.items():
            inventory.loc[mask, field] = value
    inventory.to_csv(year_dir / "metadata/table_inventory.csv", index=False)
    column_provenance = []
    columns = pd.read_csv(year_dir / "metadata/table_columns.csv", dtype=str, keep_default_na=False)
    for row in columns.to_dict("records"):
        info = lookup[row["table_name"].upper()]
        row.update({"release_status": info["release_status"], "release_date": info["release_date"], "origin": info["origin"],
                    "source_csv_sha256": info["effective_csv_sha256"],
                    "source_archive_sha256": info.get("data_archive", {}).get("archive_sha256", ACCESS_SHA),
                    "source_member_sha256": info.get("data_member", {}).get("sha256", info["original_csv_sha256"])})
        column_provenance.append(row)
    pd.DataFrame(column_provenance).to_csv(root / "column_provenance.csv", index=False)
    write_json(root / "table_provenance.json", tables)
    write_json(root / "dictionary_differences.json", metadata_differences)
    evidence = root / "evidence"
    evidence.mkdir()
    for name in ["official_release_evidence.json", "release_inventory.json", "archive_receipt.json", "access_vs_csv_parity.json"]:
        shutil.copyfile(audit / name, evidence / name)
    artifacts = [{"path": str(p.relative_to(root)), "sha256": sha256(p), "bytes": p.stat().st_size} for p in sorted(root.rglob("*")) if p.is_file() and not p.is_symlink()]
    manifest = {"schema_version": 1, "snapshot_id": "2024-mixed-source-v1", "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "year": 2024, "release_status": "Mixed: final fall overlays except original provisional dimensioned C_A; provisional winter and spring", "tables": tables,
                "policy": "Original Access retained unchanged. Fall _rv members selected explicitly. Item-status fields retained as keyed source sidecars. Original Access columns only in effective tables.",
                "artifacts": artifacts}
    write_json(root / "source_manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-root", type=Path, default=Path("/private/tmp/ipeds-post2023-audit-20260929"))
    parser.add_argument("--output-root", type=Path, default=Path("/private/tmp/ipeds-2024-extension/sources"))
    parser.add_argument("--cache-root", type=Path, default=Path("/private/tmp/ipeds-2024-download-cache"))
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--reuse-verified-snapshot", type=Path, help="Reuse only original Access extraction after verifying every artifact in an existing snapshot")
    args = parser.parse_args()
    if args.output_root.exists():
        manifest = verify_snapshot(args.output_root)
        print(f"Verified existing immutable snapshot: {args.output_root} ({len(manifest['artifacts'])} artifacts)")
        return
    args.output_root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".sources-building-", dir=args.output_root.parent) as temp:
        root = Path(temp)
        manifest = build_snapshot(root, args.audit_root, args.cache_root, args.download, args.reuse_verified_snapshot)
        # Extraction receipts contain their real staging paths for traceability.
        root.rename(args.output_root)
    print(f"Prepared {args.output_root}: {len(manifest['tables'])} tables; {len(manifest['artifacts'])} frozen artifacts")


if __name__ == "__main__":
    main()
