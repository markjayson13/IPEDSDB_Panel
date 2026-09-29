"""Fill evidenced, exact-year label gaps without rewriting source metadata.

Apply after ``build_export_metadata`` and before year-scope rendering or
observation validation. Original records remain in ``value_label_records``;
only the resolved view substitutes an evidenced label for a blank source row.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit

from export_metadata import _definition_identity, _number, _same_scope, _source_table, _text


DEFAULT_REGISTRY = (Path(__file__).resolve().parents[1] / "contracts/source_metadata_corrections"
                    / "2026-09-exact-year-labels-v1.json")
SOURCE = "official_nces_dictionary_supplement"
CODE_ISSUES = {"value_labels_missing", "value_label_coverage_incomplete",
               "value_label_incomplete", "value_label_conflict"}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _title(value) -> str:
    return " ".join(_text(value).split()).casefold()


def _identity(record: dict) -> tuple:
    return (*_definition_identity(record), _number(record.get("varnumber")))


def _number_scope(record: dict) -> tuple:
    year, source, table, _, number = _identity(record)
    return year, source, table, number


def _matches_code(record: dict, candidate: dict, names_by_number: dict) -> bool:
    if (_number_scope(record) != _number_scope(candidate)
            or _text(record.get("codevalue")) != candidate["codevalue"]):
        return False
    name = _text(record.get("varname")).upper()
    return (name == candidate["varname"] if name else
            names_by_number[_number_scope(record)] == {candidate["varname"]})


def load_supplement(path: Path = DEFAULT_REGISTRY) -> tuple[dict, dict, str]:
    """Verify the small versioned registry and its bound evidence manifest."""
    path = Path(path)
    registry = json.loads(path.read_text())
    manifest_name = Path(registry.get("evidence_manifest", ""))
    if manifest_name.name != str(manifest_name) or not manifest_name.name:
        raise ValueError("Supplement evidence must be a sibling file")
    manifest_path = path.parent / manifest_name
    if _sha(manifest_path) != registry.get("evidence_manifest_sha256"):
        raise ValueError("Supplement evidence manifest checksum mismatch")
    evidence = json.loads(manifest_path.read_text())
    if (registry.get("schema_version") != 1 or evidence.get("schema_version") != 1
            or not registry.get("supplement_id")
            or registry["supplement_id"] != evidence.get("supplement_id")):
        raise ValueError("Unsupported or mismatched supplement version")
    seen = set()
    for definition in registry["definitions"]:
        identity = _identity(definition)
        source = evidence["sources"][definition["evidence_id"]]
        url = urlsplit(source["url"])
        if (not all(identity) or not _text(definition.get("varTitle"))
                or source["year"] != definition["year"]
                or source["access_table_name"] != _source_table(definition)
                or url.scheme != "https" or url.hostname != "nces.ed.gov"
                or not url.path.startswith("/ipeds/datacenter/data/")
                or not all(re.fullmatch(r"[0-9a-f]{64}", source.get(key, ""))
                           for key in ("archive_sha256", "workbook_sha256"))):
            raise ValueError("Incomplete or mismatched supplement source identity")
        for code in definition["codes"]:
            key = (*identity, _text(code.get("codevalue")))
            if (key in seen or code.get("codevalue") is None or not _text(code.get("valuelabel"))
                    or not str(code.get("sheet", "")).startswith("Frequencies")
                    or not isinstance(code.get("row"), int) or code["row"] < 2):
                raise ValueError("Duplicate, blank, or unevidenced supplement code")
            seen.add(key)
    return registry, evidence, _sha(path)


def _record(definition: dict, code: dict, registry: dict, evidence: dict, checksum: str) -> dict:
    source = evidence["sources"][definition["evidence_id"]]
    return {**{key: definition[key] for key in
               ("year", "source_file", "access_table_name", "varname", "varnumber", "varTitle")},
            "codevalue": code["codevalue"], "valuelabel": code["valuelabel"],
            "source": SOURCE, "label_scope": "regular", "is_imputation_label": False,
            "metadata_supplement_id": registry["supplement_id"],
            "metadata_supplement_sha256": checksum,
            "metadata_supplement_evidence_sha256": registry["evidence_manifest_sha256"],
            "evidence_url": source["url"], "source_archive_sha256": source["archive_sha256"],
            "source_workbook": source["workbook"], "source_workbook_sha256": source["workbook_sha256"],
            "source_sheet": code["sheet"], "source_row": code["row"],
            "source_codevalue": code["source_codevalue"], "source_varlist_row": definition["varlist_row"]}


def _covers(code: dict, definition: dict) -> bool:
    if not _same_scope(code, definition):
        return False
    if _text(code.get("varname")):
        return _text(code["varname"]).upper() == _text(definition.get("varname")).upper()
    if code.get("label_scope") == "imputation_variable":
        return bool(definition.get("imputationvar"))
    return bool(_number(definition.get("varnumber"))) and _number(code.get("varnumber")) == _number(definition["varnumber"])


def _refresh_code_issues(metadata: dict, variable: dict) -> None:
    """Recompute only code diagnostics, preserving identity and other failures."""
    name = variable["name"]
    old = [issue for issue in metadata["issues"] if issue.get("variable") == name and issue["code"] in CODE_ISSUES]
    metadata["issues"] = [issue for issue in metadata["issues"] if issue not in old]

    def issue(code: str, message: str) -> None:
        metadata["issues"].append({"severity": "warning", "code": code, "variable": name, "message": message})

    records = variable.get("resolved_value_label_records", [])
    definitions = variable.get("source_metadata", []) or variable.get("imputation_parent_metadata", [])
    uncovered = [row for row in definitions if not any(_covers(code, row) for code in records)]
    if uncovered:
        issue("value_label_coverage_incomplete", "Code labels remain unavailable for some selected year/source definitions after supplementation.")
    mapping = defaultdict(set)
    for row in records:
        if row.get("codevalue") is None or not _text(row.get("valuelabel")):
            issue("value_label_incomplete", "A resolved code record still lacks a code or label.")
        else:
            mapping[_text(row["codevalue"])].add(_text(row["valuelabel"]))
    if not records:
        issue("value_labels_missing", "No safely resolved value labels are available.")
    conflicts = any(len(labels) > 1 for labels in mapping.values())
    if conflicts:
        issue("value_label_conflict", "Code meanings differ across selected years or sources; original definitions are retained.")
        variable["comparability_status"] = "conflicting"
        metadata["comparability_status"] = "conflicting"
    blocked = uncovered or any(issue.get("variable") == name and issue["code"] in {
        "value_label_identity_ambiguous", "value_label_source_unmatched", "lineage_metadata_incomplete",
        "value_label_incomplete", "metadata_supplement_conflict",
    } for issue in metadata["issues"])
    variable["value_labels"] = ([] if conflicts or blocked else
                                [{"value": value, "label": next(iter(labels))} for value, labels in sorted(mapping.items())])
    remaining = {issue["code"] for issue in metadata["issues"] if issue.get("variable") == name}
    for original in old:
        if original["code"] not in remaining:
            audit = {**original, "resolution": "verified_exact_year_supplement", "original_issue": original}
            if audit not in metadata.setdefault("supplement_resolved_issues", []):
                metadata["supplement_resolved_issues"].append(audit)


def apply_export_metadata_supplement(metadata: dict, registry_path: Path = DEFAULT_REGISTRY) -> dict:
    """Apply verified missing labels, in place; rerun observation checks afterward.

An exact source definition is required. Existing nonblank meanings cannot be
overwritten. Embedded supplemental rows must still match the versioned proof;
an altered or unrecognized claimed supplement fails closed.
"""
    registry, evidence, checksum = load_supplement(registry_path)
    candidates = defaultdict(list)
    expected = {}
    for definition in registry["definitions"]:
        for code in definition["codes"]:
            row = _record(definition, code, registry, evidence, checksum)
            candidates[_identity(definition)].append(row)
            expected[(*_identity(row), row["codevalue"])] = row
    affected = set()
    verified_embedded = False
    names_by_number = defaultdict(set)
    for variable in metadata["variables"]:
        for definition in variable.get("source_metadata", []) + variable.get("code_identity_candidates", []):
            names_by_number[_number_scope(definition)].add(_text(definition.get("varname")).upper())
    for variable in metadata["variables"]:
        raw = variable.setdefault("value_label_records", [])
        resolved = variable.setdefault("resolved_value_label_records", [])
        for row in [*raw, *resolved]:
            if row.get("source") != SOURCE and not row.get("metadata_supplement_id"):
                continue
            proof = expected.get((*_identity(row), _text(row.get("codevalue"))))
            if proof is None or any(row.get(key) != value for key, value in proof.items()):
                raise ValueError("Embedded metadata supplement does not match its versioned proof")
            verified_embedded = True
        if any(issue.get("variable") == variable["name"] and issue["code"] in {
            "value_label_identity_ambiguous", "value_label_source_unmatched", "lineage_metadata_incomplete",
            "variable_metadata_missing",
        } for issue in metadata["issues"]):
            continue
        sources = defaultdict(list)
        for definition in variable.get("source_metadata", []):
            sources[_identity(definition)].append(definition)
        for identity, definitions in sources.items():
            for candidate in candidates.get(identity, []):
                if any(_title(row.get("varTitle")) != _title(candidate["varTitle"]) for row in definitions):
                    continue
                if names_by_number[_number_scope(candidate)] != {candidate["varname"]}:
                    continue
                existing = [row for row in [*raw, *resolved]
                            if _matches_code(row, candidate, names_by_number)]
                known = {_text(row.get("valuelabel")) for row in existing if _text(row.get("valuelabel"))}
                if known - {candidate["valuelabel"]}:
                    issue = {"severity": "warning", "code": "metadata_supplement_conflict",
                             "variable": variable["name"], "message": "Existing nonblank source label conflicts with an exact-year supplement; neither meaning was overwritten.",
                             "year": identity[0], "codevalue": candidate["codevalue"]}
                    if issue not in metadata["issues"]:
                        metadata["issues"].append(issue)
                    affected.add(variable["name"])
                    continue
                if existing and all(_text(row.get("valuelabel")) for row in existing):
                    continue
                if candidate not in raw:
                    raw.append(dict(candidate))
                resolved[:] = [row for row in resolved
                               if not _matches_code(row, candidate, names_by_number) or _text(row.get("valuelabel"))]
                if candidate not in resolved:
                    resolved.append(dict(candidate))
                affected.add(variable["name"])
        if variable["name"] in affected:
            _refresh_code_issues(metadata, variable)
    if not affected and not verified_embedded:
        return metadata
    metadata["metadata_supplement"] = {
        "supplement_id": registry["supplement_id"], "sha256": checksum,
        "evidence_manifest_sha256": registry["evidence_manifest_sha256"],
        "policy": "exact_year_source_table_variable_number_and_title; missing_or_blank_labels_only",
    }
    if not affected:
        return metadata
    for variable in metadata["variables"]:
        if variable["name"] in affected:
            available = variable.get("metadata_availability", {})
            variable["metadata_status"] = ("complete" if available.get("dictionary") and available.get("codes")
                                           and not any(issue.get("variable") == variable["name"] for issue in metadata["issues"])
                                           else "incomplete")
    metadata["metadata_status"] = ("complete" if not metadata["issues"] and all(
        variable["metadata_status"] == "complete" for variable in metadata["variables"]) else "incomplete")
    if "readiness_status" in metadata:
        metadata["readiness_status"] = "incomplete"  # Caller must revalidate observations and format.
    return metadata
