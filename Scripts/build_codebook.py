#!/usr/bin/env python3
"""Build the public codebook from the two verified, published metadata companions.

Reads metadata and file bytes for checksums only; never reads institution records
or modifies a release. Run with --root /Volumes/CIRAGO/IPEDSDB_PANEL.
"""
from __future__ import annotations

import argparse
import csv
import gc
import gzip
import hashlib
import io
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

PANEL = "panel_clean_prch_2004_2023"
MAX_ASSET_BYTES = 5 * 1024 * 1024
SHARD_BYTES = 4 * 1024 * 1024
SCHEMA_VERSION = 1


def compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_hash(path: Path, expected: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", expected or ""):
        raise ValueError(f"Missing or invalid expected SHA-256: {path.name}")
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch: {path.name}; expected {expected}, got {actual}")
    return actual


def text(value: Any) -> str:
    return "" if value is None else str(value)


def first(record: dict, *keys: str) -> Any:
    return next((record[k] for k in keys if record.get(k) not in (None, "")), "")


def years(value: Any) -> list[int]:
    if value is None or value == "":
        return []
    if not isinstance(value, list):
        value = [value]
    return sorted({int(year) for year in value})


def grouped(records: list[dict]) -> list[dict]:
    """Collapse identical non-year fields; never fill gaps in year coverage."""
    groups: dict[str, dict] = {}
    for record in records:
        payload = {k: v for k, v in record.items() if k not in {"year", "years"}}
        key = compact(payload)
        entry = groups.setdefault(key, {**payload, "years": set()})
        entry["years"].update(years(record.get("years", record.get("year"))))
    result = [{**v, "years": sorted(v["years"])} for v in groups.values()]
    return sorted(result, key=lambda r: (r["years"][0] if r["years"] else -1, compact(r)))


def source_record(record: dict) -> dict:
    result = {
        "year": record.get("year"),
        "source_file": text(record.get("source_file")),
        "table": text(first(record, "resolved_physical_table", "access_table_name")),
        "varname": text(record.get("varname")),
        "varnumber": text(record.get("varnumber")),
        "title": text(first(record, "varTitle", "title")),
        "description": text(first(record, "longDescription", "description")),
        "reference_period": text(record.get("reference_period")),
        "imputationvar": text(record.get("imputationvar")),
        "original_table": text(record.get("original_access_table_name")),
        "correction_id": text(record.get("metadata_correction_id")),
        "correction_reason": text(record.get("metadata_correction_reason")),
    }
    for key in ("academic_year_label", "release_type", "source_table_reference_period",
                "reference_period_start", "reference_period_end", "imputation_flag_availability",
                "source_archive_sha256", "source_database_sha256", "source_physical_table_sha256",
                "metadata_correction_registry_sha256", "metadata_correction_evidence",
                "source_release_date", "source_release_status", "source_url",
                "source_member", "source_member_sha256", "source_snapshot_id",
                "reporting_population_note", "reporting_population_source"):
        if record.get(key) not in (None, ""):
            result[key] = record[key]
    return result


def code_records(records: list[dict]) -> list[dict]:
    # Sources are collected within each actual code/label/year, before grouping
    # across years. Grouping by code alone would invent source/year combinations.
    scopes: dict[tuple, set] = defaultdict(set)
    for record in records:
        key = (record.get("year"), text(record.get("codevalue")), text(record.get("valuelabel")))
        if record.get("source_file"):
            scopes[key].add(text(record["source_file"]))
        else:
            scopes[key]  # Keep a documented blank record visible.
    rows = [{"year": year, "code": code, "label": label, "sources": sorted(sources)}
            for (year, code, label), sources in scopes.items()]
    return grouped(rows)


def public_issue(issue: dict) -> dict:
    # No institution IDs, record rows, private paths, or arbitrary diagnostics.
    result = {k: issue[k] for k in ("variable", "code", "message", "count", "severity", "resolution")
              if k in issue}
    if "examples" in issue:
        result["examples"] = [
            {k: example[k] for k in ("year", "value") if k in example}
            for example in issue["examples"] if isinstance(example, dict)
        ]
    return result


def compact_definitions(detail: dict) -> dict:
    """Store each definition once per variable, with exact source year scopes.

    Line breaks and repeated whitespace in Access text are presentation details;
    normalizing them lets source records reference the identical scoped meaning.
    """
    normalize = lambda value: " ".join(text(value).split())
    definitions = grouped([
        {"years": row["years"], "label": normalize(row["label"]),
         "description": normalize(row["description"])}
        for row in detail["definitions"]
    ])
    lookup = {(d["label"], d["description"]): i for i, d in enumerate(definitions)}
    sources = []
    for row in detail["source_records"]:
        key = (normalize(row["title"]), normalize(row["description"]))
        if key not in lookup:
            lookup[key] = len(definitions)
            definitions.append({"label": key[0], "description": key[1], "years": row["years"]})
        definition_index = lookup[key]
        definitions[definition_index]["years"] = sorted(
            set(definitions[definition_index]["years"]) | set(row["years"]))
        sources.append({**{k: v for k, v in row.items() if k not in {"title", "description"}},
                        "definition_index": definition_index})
    detail["definitions"] = definitions
    detail["source_records"] = grouped(sources)
    detail["description"] = ""  # All text, including blanks, is in definitions.
    return detail


def make_detail(variable: dict, issues: list[dict]) -> dict:
    records = variable.get("source_metadata", [])
    sources = sorted({text(r["source_file"]) for r in records if r.get("source_file")})
    definitions = variable.get("year_scoped_definitions")
    if not definitions:
        definitions = [{"year": r.get("year"), "label": first(r, "varTitle", "title"),
                        "description": first(r, "longDescription", "description")} for r in records]
    if not definitions:
        definitions = [{"years": variable.get("observed_years", []), "label": variable.get("label"),
                        "description": variable.get("description")}]
    definitions = grouped([{k: (text(v) if k in {"label", "description"} else v)
                            for k, v in d.items()} for d in definitions])
    resolved = variable.get("resolved_value_label_records") or variable.get("value_label_records", [])
    codes = code_records(resolved)
    provenance = []
    for record in resolved:
        if record.get("metadata_supplement_id"):
            provenance.append({k: record[k] for k in (
                "year", "source_file", "access_table_name", "varname", "varnumber", "source",
                "metadata_supplement_id", "metadata_supplement_sha256", "evidence_url",
                "source_archive_sha256", "source_workbook_sha256", "metadata_supplement_evidence_sha256",
            ) if k in record})
    original = variable.get("value_label_records", [])
    original_codes = code_records(original) if provenance else []
    result = {
        "name": variable["name"], "label": text(variable.get("label")),
        "storage_type": variable["storage_type"], "stata_name": variable["name"],
        "observed_years": years(variable.get("observed_years")), "sources": sources,
        "metadata_status": variable.get("metadata_status", "unknown"),
        "has_codes": bool(codes), "has_issues": bool(issues),
        "null_count": variable.get("null_count"), "description": text(variable.get("description")),
        "definitions": definitions, "codes": codes,
        "source_records": grouped([source_record(r) for r in records]),
        "semantic_metadata": variable.get("semantic_metadata", {}), "issues": issues,
        "comparability_status": variable.get("comparability_status", "unknown"),
        "metadata_origin": variable.get("metadata_origin", "unknown"),
    }
    if provenance:
        result["code_provenance"] = grouped(provenance)
        result["original_codes"] = original_codes
    if variable.get("column_consolidation"):
        result["column_consolidation"] = variable["column_consolidation"]
    return compact_definitions(result)


def parquet_codebook(metadata: dict) -> tuple[dict, dict[str, dict]]:
    issues = [public_issue(issue) for issue in metadata.get("issues", [])]
    by_variable: dict[str, list] = defaultdict(list)
    for issue in issues:
        by_variable[issue.get("variable", "")].append(issue)
    details = {}
    for variable in metadata["variables"]:
        name = variable["name"]
        if name in details:
            raise ValueError(f"Duplicate panel variable: {name}")
        details[name] = make_detail(variable, by_variable[name])
    if len(details) != metadata["column_count"]:
        raise ValueError("Metadata column count does not match variable records")
    index = {
        "schema_version": SCHEMA_VERSION, "title": "IPEDS panel codebook",
        "release": "full-panel-labels-v1", "years": years(metadata["years"]),
        "row_count": metadata["row_count"], "column_count": metadata["column_count"],
        "panel_keys": metadata["panel_keys"], "issues": issues,
        "sources": sorted({source for d in details.values() for source in d["sources"]}),
        "readiness_status": metadata.get("readiness_status", "unknown"),
        "metadata_supplement": metadata.get("metadata_supplement", {}),
        "source_metadata_sha256": metadata.get("metadata_source_sha256", {}),
        "notes": [
            "This codebook describes the published institution-year panel; it contains no institution-level data.",
            "Reporting year is the panel key. Individual measures may refer to a preceding academic or fiscal period; use their source records.",
            "Year lists are exact. A meaning documented in one year must not be assumed for another year.",
            "Blank definitions and labels mean Not supplied by the verified source. Undocumented observed category values remain unresolved.",
            "Nulls are missing observations, distinct from negative or special source codes. Ordinary string nulls become empty strings in Stata.",
            "Stata may encode categorical strings as numbers. The Stata mapping preserves the original source token and native export label.",
            f"The full dataset has {metadata['column_count']:,} variables. Stata/BE requires a selected variable list; Stata/SE or MP can open the whole file.",
            "Metadata completeness does not establish comparability across years. Definitions, units, price basis and reference periods must be considered for each analysis.",
        ],
    }
    return index, details


def add_stata(index: dict, details: dict[str, dict], metadata: dict) -> None:
    if metadata["row_count"] != index["row_count"] or years(metadata["years"]) != index["years"]:
        raise ValueError("Parquet and Stata metadata describe different panel rows or years")
    stata_variables = {v["name"]: v for v in metadata["variables"]}
    if len(stata_variables) != len(metadata["variables"]) or set(stata_variables) != set(details):
        raise ValueError("Parquet and Stata metadata variable identities differ")
    for name, detail in details.items():
        v = stata_variables[name]
        if v.get("null_count") != detail["null_count"] or years(v.get("observed_years")) != detail["observed_years"]:
            raise ValueError(f"Parquet and Stata metadata observation coverage differs: {name}")
        detail["stata_name"] = v.get("export_name", name)
        detail["stata"] = {
            "name": v.get("export_name", name), "label": text(v.get("export_label")),
            "storage_conversion": v.get("stata_storage_conversion", "none"),
            "source_code_map": v.get("stata_source_code_map", []),
            "value_labels": v.get("stata_value_labels", {}),
        }


def summary(detail: dict, filename: str) -> dict:
    keys = ("name", "label", "storage_type", "stata_name", "observed_years", "sources",
            "metadata_status", "has_codes", "has_issues", "null_count")
    return {**{key: detail[key] for key in keys}, "detail_file": filename}


def dictionary_csv_row(detail: dict) -> dict:
    definitions = detail["definitions"]
    latest = max(definitions, key=lambda d: max(d["years"], default=-1), default={})
    latest_year = max(latest.get("years", []), default=None)
    candidates = [d for d in definitions if latest_year in d["years"]] if latest_year is not None else definitions
    description = latest.get("description", "")
    description_years = latest.get("years", [])
    if len(candidates) > 1:
        # A shared latest year is not permission to discard a source definition,
        # especially a blank one. Keep every variant explicit and narrow the
        # summary's scope to the year in which those records coexist.
        candidates = sorted(candidates, key=lambda d: (d.get("label", ""), d.get("description", "")))
        scope = str(latest_year) if latest_year is not None else "unspecified years"
        description = f"Multiple source definitions for {scope}; blank descriptions remain unresolved.\n" + "\n".join(
            f"{number}. {d.get('label') or 'Title not supplied'}: {d.get('description') or 'Description not supplied'}"
            for number, d in enumerate(candidates, 1)
        )
        description_years = [latest_year] if latest_year is not None else []
    return {
        "name": detail["name"], "label": detail["label"], "storage_type": detail["storage_type"],
        "stata_name": detail["stata_name"],
        "observed_years": "; ".join(map(str, detail["observed_years"])),
        "sources": "; ".join(detail["sources"]), "metadata_status": detail["metadata_status"],
        "null_count": detail["null_count"], "description": description,
        "description_years": "; ".join(map(str, description_years)),
        "issues": " | ".join(f"{issue['code']}: {issue['message']}" for issue in detail["issues"]),
    }


def write_csv(output: Path, name: str, fields: list[str], rows: list[dict]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows({key: compact(value) if isinstance(value, (list, dict)) else value
                     for key, value in row.items()} for row in rows)
    content = buffer.getvalue().encode("utf-8-sig")
    filename = name
    if len(content) >= MAX_ASSET_BYTES:
        content = gzip.compress(content, mtime=0)
        filename += ".gz"
    if len(content) >= MAX_ASSET_BYTES:
        raise ValueError(f"Compressed codebook asset exceeds 5 MiB: {filename}")
    (output / filename).write_bytes(content)
    alternative = output / (name if filename != name else name + ".gz")
    alternative.unlink(missing_ok=True)
    return filename


def write_assets(index: dict, details: dict[str, dict], output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    shards: list[dict] = []
    shard: dict = {}
    size = 2
    for name in sorted(details):
        entry_size = len(compact({name: details[name]}).encode("utf-8"))
        if entry_size >= SHARD_BYTES:
            raise ValueError(f"Variable exceeds shard size limit: {name}")
        if shard and size + entry_size >= SHARD_BYTES:
            shards.append(shard)
            shard, size = {}, 2
        shard[name] = details[name]
        size += entry_size
    if shard:
        shards.append(shard)
    index["variables"] = []
    filenames = set()
    for number, shard in enumerate(shards, 1):
        filename = f"variables-{number:03}.json"
        filenames.add(filename)
        for detail in shard.values():
            index["variables"].append(summary(detail, filename))
        (output / filename).write_text(compact(shard) + "\n", encoding="utf-8")
    for old in output.glob("variables-*.json"):
        if old.name not in filenames:
            old.unlink()
    csv_fields = ["name", "label", "storage_type", "stata_name", "observed_years", "sources",
                  "metadata_status", "null_count", "description", "description_years", "issues"]
    csv_name = write_csv(output, "codebook.csv", csv_fields,
                         [dictionary_csv_row(details[name]) for name in sorted(details)])
    definition_fields = ["variable", "definition_index", "years", "label", "description"]
    definitions_name = write_csv(output, "definitions.csv", definition_fields, [
        {"variable": name, "definition_index": number, "years": "; ".join(map(str, definition["years"])),
         "label": definition["label"], "description": definition["description"]}
        for name in sorted(details) for number, definition in enumerate(details[name]["definitions"])
    ])
    code_fields = ["variable", "code", "label", "years", "sources"]
    labels_name = write_csv(output, "value-labels.csv", code_fields,
                           [{"variable": name, **code} for name in sorted(details) for code in details[name]["codes"]])
    index["downloads"] = {"dictionary": csv_name, "definitions": definitions_name, "value_labels": labels_name,
                          "pdf": "ipeds-panel-codebook.pdf"}
    consolidated = [detail for detail in details.values() if detail.get("column_consolidation")]
    if consolidated:
        index["downloads"]["column_crosswalk"] = write_csv(
            output, "column-crosswalk.csv", ["canonical_name", "original_column", "years", "rationale", "caveats"],
            [{"canonical_name": detail["name"], "original_column": member["column"],
              "years": member["years"], "rationale": detail["column_consolidation"].get("rationale", ""),
              "caveats": detail["column_consolidation"].get("caveats", [])}
             for detail in consolidated for member in detail["column_consolidation"]["members"]])
    data = (compact(index) + "\n").encode("utf-8")
    if len(data) >= MAX_ASSET_BYTES:
        raise ValueError("Codebook index exceeds 5 MiB")
    (output / "index.json").write_bytes(data)


def load_verified_metadata(path: Path, expected: str) -> tuple[dict, str]:
    data = path.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch: {path.name}")
    return json.loads(data), actual


def build(root: Path, output: Path, *, manifest_path: Path | None = None,
          panel_relative: str | None = None) -> dict:
    """Build from a hash-bound manifest; defaults retain the existing Final API.

    Explicit candidates must supply both options. Their artifact paths are
    relative to root, so provisional documentation never falls back to Final.
    """
    if (manifest_path is None) != (panel_relative is None):
        raise ValueError("Supply both manifest_path and panel_relative for a candidate")
    manifest_path = manifest_path or root / "Final/manifest.json"
    panel_relative = panel_relative or f"Final/{PANEL}"
    relative_path = Path(panel_relative)
    if relative_path.is_absolute() or ".." in relative_path.parts or relative_path.suffix:
        raise ValueError("Panel path must be a safe relative stem without an extension")
    manifest = json.loads(manifest_path.read_text())
    artifacts = {record["path"]: record["sha256"] for record in manifest["artifacts"]}
    fingerprints = {}
    index, details = {}, {}
    for ext in ("parquet", "dta"):
        relative = f"{panel_relative}.{ext}"
        metadata_relative = relative + ".metadata.json"
        if relative not in artifacts or metadata_relative not in artifacts:
            raise ValueError(f"Published manifest does not list data and metadata: {ext}")
        metadata, metadata_hash = load_verified_metadata(root / metadata_relative, artifacts[metadata_relative])
        if metadata.get("data_sha256") != artifacts[relative]:
            raise ValueError(f"Metadata data_sha256 differs from published manifest: {ext}")
        data_hash = verify_hash(root / relative, artifacts[relative])
        key = "stata" if ext == "dta" else "parquet"
        fingerprints[f"{key}_sha256"] = data_hash
        fingerprints[f"{key}_metadata_sha256"] = metadata_hash
        if ext == "parquet":
            index, details = parquet_codebook(metadata)
        else:
            add_stata(index, details, metadata)
        del metadata
        gc.collect()
    index["release"] = manifest.get("release_name", manifest.get("labeled_panel_release", index["release"]))
    index["data_directory"] = str(relative_path.parent)
    index["release_status"] = manifest.get("source_release_status", "Final")
    if index["release_status"] != "Final":
        index["notes"].insert(0, f"Source release status: {index['release_status']}. Consult each variable's source release and date.")
    notes = manifest.get("codebook_notes", [])
    if not isinstance(notes, list) or any(not isinstance(note, str) for note in notes):
        raise ValueError("Manifest codebook_notes must be a list of strings")
    index["notes"].extend(notes)
    index["release_notes"] = notes
    index["codebook_url"] = manifest.get("codebook_url", "https://markjayson13.github.io/IPEDSDB_Panel/")
    index["generated_from"] = fingerprints
    write_assets(index, details, output)
    return {"variables": len(details), "issues": len(index["issues"]), "generated_from": fingerprints,
            "downloads": index["downloads"], "shards": len({v["detail_file"] for v in index["variables"]})}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="Published IPEDSDB_PANEL output root")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parents[1] / "docs/codebook")
    parser.add_argument("--manifest", type=Path, help="Explicit hash-bound candidate manifest; requires --panel-relative")
    parser.add_argument("--panel-relative", help="Panel stem relative to --root, without .parquet/.dta; requires --manifest")
    args = parser.parse_args()
    print(json.dumps(build(args.root, args.output_dir, manifest_path=args.manifest,
                           panel_relative=args.panel_relative), indent=2))


if __name__ == "__main__":
    main()
