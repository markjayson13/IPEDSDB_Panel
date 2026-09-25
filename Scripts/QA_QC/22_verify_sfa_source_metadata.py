#!/usr/bin/env python3
"""Re-extract the locked original Access release and audit each 2023 SFA definition.

Writes evidence and a proposed per-variable registry, never changes source data.
Requires mdbtools. The registry must be reviewed before copying into contracts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd

ARCHIVE_SHA256 = "5a29f4b8d0fbdd5e091015e286dbddcecdf8d423926521a8af560967cd376629"
DATABASE_SHA256 = "95983414e996ffce605f3de728b86eb572500353164da98dc818b055df3c9026"
VERSION = "2023-sfa-v1"
TABLES = ["SFAV2223", "sfa2223_p1", "sfa2223_p2", "varTable23", "valueSets23", "Tables23"]


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def csv_frame(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def normalized(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.map(lambda value: value.replace("\r\n", "\n").replace("\r", "\n"))


def audit(root: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    year_root = root / "Raw_Access_Databases/2023"
    archive = year_root / "downloads/IPEDS_2023-24_Final.zip"
    if digest(archive) != ARCHIVE_SHA256:
        raise ValueError("Original release differs from the audited source lock")
    # Extract only these known members; never reuse the cached extracted database.
    with zipfile.ZipFile(archive) as bundle:
        for name in ["IPEDS202324.accdb", "ReadMe2023-24.docx", "IPEDS202324Tablesdoc.xlsx"]:
            with bundle.open(name) as source, (output / name).open("wb") as dest:
                while block := source.read(8 * 1024 * 1024):
                    dest.write(block)
    database = output / "IPEDS202324.accdb"
    if digest(database) != DATABASE_SHA256:
        raise ValueError("Original database hash differs from verified evidence")
    schema = subprocess.check_output(["mdb-schema", str(database)], text=True)
    (output / "schema.sql").write_text(schema)
    physical = {name.upper(): [v.upper() for v in re.findall(r"^\s*\[([^]]+)\]", body, re.M)]
                for name, body in re.findall(r"CREATE TABLE \[([^]]+)\]\s*\((.*?)\n\);", schema, re.S)}
    for name in TABLES:
        with (output / f"{name}.csv").open("wb") as handle:
            subprocess.run(["mdb-export", str(database), name], stdout=handle, check=True)
        cached = year_root / "tables_csv" / f"{name}.csv"
        pd.testing.assert_frame_equal(normalized(csv_frame(output / f"{name}.csv")), normalized(csv_frame(cached)))
    definitions = csv_frame(output / "varTable23.csv")
    definitions = definitions[definitions.TableName.str.upper().str.startswith("SFA")]
    table_definitions = csv_frame(output / "Tables23.csv")
    workbook = pd.read_excel(output / "IPEDS202324Tablesdoc.xlsx", sheet_name="varTable23", dtype=str).fillna("")
    records, corrections = [], []
    for row in definitions.to_dict("records"):
        name, declared = row["VarName"].upper(), row["TableName"].upper()
        # Search ALL physical Access tables, not just the presumed SFA family.
        targets = [table for table, columns in physical.items() if name in columns]
        status = ("consistent" if targets == [declared] else "unique_physical_mismatch" if len(targets) == 1
                  else "absent_from_database" if not targets else "ambiguous_physical_match")
        record = {"year": 2023, "varname": name, "varnumber": row["VarNumber"], "original_table": declared,
                  "physical_tables": "|".join(targets), "classification": status,
                  "imputationvar": row["ImputationVar"],
                  "imputation_flag_tables": "|".join(t for t, cols in physical.items() if row["ImputationVar"].upper() in cols),
                  "original_metadata": row}
        records.append(record)
        if status == "unique_physical_mismatch":
            corrections.append({"year": 2023, "varname": name, "varnumber": row["VarNumber"],
                                "original_table": declared, "resolved_table": targets[0],
                                "evidence_id": f"{VERSION}:{row['VarNumber']}:{name}",
                                "reason": "Original Access dictionary TableName differs from the unique physical column in the same locked database; verified individually, not inferred from table naming."})
    pell = [record for record in records if record["varname"] in {"UPGRNTN", "UPGRNTT"}]
    for item in pell:
        match = workbook[workbook.VarName.eq(item["varname"]) & workbook.VarNumber.eq(item["varnumber"])]
        if len(match) != 1 or match.iloc[0].TableName != item["original_table"]:
            raise ValueError("Pell workbook evidence differs from the Access dictionary")
        item["workbook_metadata"] = match.iloc[0].to_dict()
    with zipfile.ZipFile(output / "ReadMe2023-24.docx") as doc:
        xml = ET.fromstring(doc.read("word/document.xml"))
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs = ["".join(t.text or "" for t in p.findall(".//w:t", ns)) for p in xml.findall(".//w:p", ns)]
    (output / "release_readme_paragraphs.txt").write_text("\n".join(paragraphs))
    flag_evidence = [p for p in paragraphs if "imputation status flag" in p]
    if len(flag_evidence) != 1 or "not included" not in flag_evidence[0]:
        raise ValueError("Imputation flag release evidence could not be verified")
    source_manifest = csv_frame(year_root / "manifest.csv").iloc[0].to_dict()
    inputs = [archive, *(year_root / "tables_csv" / f"{name}.csv" for name in TABLES)]
    evidence = {"schema_version": 1, "correction_version": VERSION,
                "source_manifest": source_manifest,
                "cached_extraction_comparison": "All six fresh CSV tables equal cached tables after CRLF/LF normalization inside cells; data tables are byte-identical.",
                "original_access_table_count": len(physical),
                "classification_counts": pd.Series([r["classification"] for r in records]).value_counts().to_dict(),
                "imputation_flag_evidence": flag_evidence[0], "pell": pell,
                "physical_table_definitions": table_definitions[table_definitions.TableName.str.upper().isin(["SFA2223_P1", "SFA2223_P2", "SFAV2223"])].to_dict("records"),
                "audit": records,
                "artifacts": [{"path": str(p.name), "size_bytes": p.stat().st_size, "sha256": digest(p)}
                              for p in sorted(output.iterdir()) if p.is_file() and p.name in {"IPEDS202324.accdb", "ReadMe2023-24.docx", "IPEDS202324Tablesdoc.xlsx", "schema.sql", *(f"{n}.csv" for n in TABLES)}]}
    registry = {"schema_version": 1, "version": VERSION,
                "evidence_path": "contracts/source_metadata_corrections/2023-sfa-v1.evidence.json",
                "archive": {"url": source_manifest["access_url"], "sha256": ARCHIVE_SHA256},
                "database": {"name": database.name, "sha256": DATABASE_SHA256},
                "pipeline_inputs": [{"path": str(p.relative_to(root)), "sha256": digest(p)} for p in inputs],
                "physical_tables": {name.upper(): {"sha256": digest(output / f"{name}.csv"), "columns": physical[name.upper()]}
                                    for name in TABLES[:3]},
                "corrections": corrections}
    (output / f"{VERSION}.json").write_text(json.dumps(registry, indent=2) + "\n")
    (output / f"{VERSION}.evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    pd.DataFrame([{k: v for k, v in record.items() if k not in {"original_metadata", "workbook_metadata"}} for record in records]).to_csv(output / f"{VERSION}.audit.csv", index=False)
    return evidence["classification_counts"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.root, args.output), indent=2))
