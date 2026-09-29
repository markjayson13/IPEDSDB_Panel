"""Explicit, opt-in rendering of metadata meanings that change across years.

This renders available source meanings; it does not establish comparability
or fill missing definitions. All original source records remain untouched.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict


def year_scope_enabled(schema, requested: bool = False) -> bool:
    """Respect a stored panel's declared scope policy when it is re-exported."""
    embedded = (schema.metadata or {}).get(b"ipeds:export")
    if embedded is None:
        return bool(requested)
    try:
        metadata = json.loads(embedded)
        if not isinstance(metadata, dict):
            raise ValueError("expected an export metadata object")
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Cannot read embedded export scope policy: {exc}") from exc
    return bool(requested) or metadata.get("metadata_scope_policy") == "explicit_year_scopes"


def _normalized(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _year_ranges(years: list[int]) -> str:
    ordered = sorted(set(years))
    ranges = []
    for year in ordered:
        if ranges and year == ranges[-1][-1] + 1:
            ranges[-1].append(year)
        else:
            ranges.append([year])
    return ",".join(str(group[0]) if len(group) == 1 else f"{group[0]}-{group[-1]}" for group in ranges)


def _by_year(records: list[dict], field: str) -> dict[int, str] | None:
    meanings = defaultdict(set)
    for record in records:
        try:
            year = int(record["year"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return None
        meaning = _normalized(record.get(field))
        if not meaning:
            return None
        meanings[year].add(meaning)
    if not meanings or any(len(values) != 1 for values in meanings.values()):
        return None
    return {year: next(iter(values)) for year, values in sorted(meanings.items())}


def _render(meanings: dict[int, str]) -> str:
    years_by_meaning = defaultdict(list)
    for year, meaning in sorted(meanings.items()):
        years_by_meaning[meaning].append(year)
    return "; ".join(f"{_year_ranges(years)}: {meaning}" for meaning, years in years_by_meaning.items())


def apply_year_scoped_metadata(metadata: dict) -> dict:
    """Render unambiguous meanings within each year and retain the full audit.

Only label/description/code conflicts are eligible. Missing source coverage,
unknown observations, ambiguous identities and same-year conflicts continue
to block readiness. A varying variable title requires observed-year evidence
to choose the latest observed title, prefixed with a persistent scope tag.
"""
    issues = metadata.get("issues", [])
    issue_codes = defaultdict(set)
    for issue in issues:
        issue_codes[issue.get("variable")].add(issue["code"])
    resolved = set()
    for variable in metadata["variables"]:
        name = variable["name"]
        codes = issue_codes[name]
        records = variable.get("source_metadata", [])
        blocked_identity = bool(codes.intersection({"lineage_metadata_incomplete", "variable_metadata_missing"}))
        if not blocked_identity:
            titles = _by_year(records, "varTitle")
            descriptions = _by_year(records, "longDescription")
            if titles is not None or descriptions is not None:
                variable["year_scoped_definitions"] = [
                    {"year": year, "label": (titles or {}).get(year),
                     "description": (descriptions or {}).get(year)}
                    for year in sorted(set(titles or {}) | set(descriptions or {}))
                ]
            if "variable_label_conflict" in codes and titles is not None:
                if len(set(titles.values())) == 1:
                    variable["label"] = next(iter(titles.values()))
                    variable["label_scope"] = "universal_whitespace_normalized"
                    resolved.add((name, "variable_label_conflict"))
                else:
                    observed = variable.get("observed_years")
                    if observed is not None and observed and set(observed).issubset(titles):
                        variable["label"] = "[varies by year] " + titles[max(observed)]
                        variable["label_scope"] = "year_specific"
                        variable["label_reference_year"] = max(observed)
                        resolved.add((name, "variable_label_conflict"))
            if "variable_description_conflict" in codes and descriptions is not None:
                variable["description"] = _render(descriptions)
                variable["description_scope"] = "year_specific"
                resolved.add((name, "variable_description_conflict"))

        blocked_codes = blocked_identity or bool(codes.intersection({
            "value_label_identity_ambiguous", "value_label_source_unmatched",
            "value_label_coverage_incomplete", "value_label_incomplete",
        }))
        if "value_label_conflict" in codes and not blocked_codes:
            by_code = defaultdict(list)
            for record in variable.get("resolved_value_label_records", []):
                if record.get("codevalue") is None:
                    by_code = {}
                    break
                by_code[str(record["codevalue"]).strip()].append(record)
            mappings = {value: _by_year(rows, "valuelabel") for value, rows in by_code.items()}
            if mappings and all(mapping is not None for mapping in mappings.values()):
                variable["value_labels"] = [
                    {"value": value, "label": _render(mapping)}
                    for value, mapping in sorted(mappings.items())
                ]
                variable["year_scoped_value_labels"] = [
                    {"value": value, "labels_by_year": [
                        {"year": year, "label": label} for year, label in mapping.items()
                    ]}
                    for value, mapping in sorted(mappings.items())
                ]
                variable["value_label_scope"] = "year_specific"
                resolved.add((name, "value_label_conflict"))

        # Even a stable code meaning is not a universal assignment when its
        # source records omit a year that contains observations. Rendering the
        # actual code-year scopes keeps an unknown observed code from silently
        # borrowing a label supplied only for another year.
        observed = set(variable.get("observed_years") or [])
        if (variable.get("value_labels") and observed and not blocked_codes
                and variable.get("value_label_scope") != "year_specific"):
            by_code = defaultdict(list)
            for record in variable.get("resolved_value_label_records", []):
                if record.get("codevalue") is not None:
                    by_code[str(record["codevalue"]).strip()].append(record)
            mappings = {label["value"]: _by_year(by_code.get(label["value"], []), "valuelabel")
                        for label in variable["value_labels"]}
            if (mappings and all(mapping is not None for mapping in mappings.values())
                    and any(not observed.issubset(mapping) for mapping in mappings.values())):
                variable["value_labels"] = [
                    {"value": label["value"], "label": (_render(mappings[label["value"]])
                     if not observed.issubset(mappings[label["value"]]) else label["label"])}
                    for label in variable["value_labels"]
                ]
                variable["year_scoped_value_labels"] = [
                    {"value": value, "labels_by_year": [
                        {"year": year, "label": label} for year, label in mapping.items()
                    ]}
                    for value, mapping in sorted(mappings.items())
                ]
                variable["value_label_scope"] = "year_specific"

    retained = []
    scoped = metadata.setdefault("scoped_issues", [])
    for issue in issues:
        if (issue.get("variable"), issue["code"]) in resolved:
            scoped.append({**issue, "severity": "info", "original_severity": issue.get("severity"),
                           "resolution": "explicit_year_scopes", "original_issue": dict(issue)})
        else:
            retained.append(issue)
    metadata["issues"] = retained
    metadata["metadata_scope_policy"] = "explicit_year_scopes"
    remaining_names = {issue.get("variable") for issue in retained}
    resolved_names = {name for name, _ in resolved}
    for variable in metadata["variables"]:
        if variable["name"] not in resolved_names:
            continue
        available = variable.get("metadata_availability", {})
        valid = (variable.get("metadata_origin") == "controlled_panel_key"
                 or (available.get("dictionary", False) and available.get("codes", False)))
        variable["metadata_status"] = ("complete" if valid and variable["name"] not in remaining_names else "incomplete")
    metadata["metadata_status"] = ("complete" if not retained and all(
        variable["metadata_status"] == "complete" for variable in metadata["variables"]
    ) else "incomplete")
    if "readiness_status" in metadata:
        metadata["readiness_status"] = ("complete" if metadata["metadata_status"] == "complete"
                                        and metadata.get("observation_validation", {}).get("status", "complete") == "complete"
                                        and metadata.get("format_readiness_status", "complete") == "complete"
                                        else "incomplete")
    return metadata
