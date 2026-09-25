"""Shared observation checks, provenance, and recoverable export promotion.

Promotion rolls back ordinary exceptions. It is not an atomic filesystem
transaction across files: a process/machine crash can require recovery from the
retained backup directory. Consumers should wait for successful completion and
verify the data checksum in the metadata companion.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import subprocess
import tempfile
from decimal import Decimal, InvalidOperation

import pyarrow as pa
import pyarrow.dataset as ds

from export_metadata import _definition_identity, _name, _number, _same_scope, _source_table


PACKAGE_SUFFIXES = (".dictionary.csv", ".value_labels.csv", ".README.txt", ".metadata.json")


def _issue(code: str, message: str, variable: str | None = None, **detail) -> dict:
    return {"severity": "error", "code": code, "variable": variable, "message": message, **detail}


def _positive_integer(value) -> int | None:
    if value is None or isinstance(value, (bool, str, bytes)):
        return None
    try:
        number = int(value)
        return number if number > 0 and number == value else None
    except (ValueError, TypeError, OverflowError):
        return None


def scan_panel(dataset, columns: list[str], filt=None, batch_rows: int = 100_000,
               panel_keys: tuple[str, str] = ("UNITID", "year")) -> dict:
    """Inspect a projection without changing data; keep duplicate detection on disk.

Keys must be positive numeric integers. Missing keys and duplicate keys are
diagnostics, including in non-strict exports. Invalid years are not used to
select dictionary records.
    """
    unitid_col, year_col = panel_keys
    missing_keys = [name for name in panel_keys if name not in columns]
    issues = [_issue("panel_key_missing", f"Panel key column is absent: {name}.", name) for name in missing_keys]
    nulls = {name: 0 for name in columns}
    years = set()
    rows = 0
    invalid = {name: 0 for name in panel_keys}
    duplicate_count = 0
    duplicate_examples = []
    with tempfile.TemporaryDirectory(prefix="ipeds-export-keys-") as temp:
        connection = sqlite3.connect(str(Path(temp) / "keys.sqlite"))
        try:
            connection.execute("CREATE TABLE keys (unitid TEXT NOT NULL, year TEXT NOT NULL, PRIMARY KEY(unitid, year)) WITHOUT ROWID")
            for batch in dataset.to_batches(columns=columns, filter=filt, batch_size=batch_rows):
                rows += batch.num_rows
                for name, values in zip(columns, batch.columns):
                    nulls[name] += values.null_count
                if missing_keys:
                    continue
                units = batch.column(columns.index(unitid_col)).to_pylist()
                batch_years = batch.column(columns.index(year_col)).to_pylist()
                keys = []
                for unitid, year in zip(units, batch_years):
                    unit = _positive_integer(unitid)
                    numeric_year = _positive_integer(year)
                    if unit is None:
                        invalid[unitid_col] += 1
                    if numeric_year is None:
                        invalid[year_col] += 1
                    else:
                        years.add(numeric_year)
                    if unit is not None and numeric_year is not None:
                        keys.append((str(unit), str(numeric_year)))
                for key in keys:
                    before = connection.total_changes
                    connection.execute("INSERT OR IGNORE INTO keys VALUES (?, ?)", key)
                    if connection.total_changes == before:
                        duplicate_count += 1
                        if len(duplicate_examples) < 5:
                            duplicate_examples.append({unitid_col: int(key[0]), year_col: int(key[1])})
                connection.commit()
        finally:
            connection.close()
    for name, count in invalid.items():
        if count:
            issues.append(_issue("invalid_panel_key", f"{count} rows have missing or invalid {name}; keys must be positive numeric integers.", name, count=count))
    if duplicate_count:
        issues.append(_issue("duplicate_panel_key", f"{duplicate_count} duplicate institution-year rows occur after the first observation.", count=duplicate_count, examples=duplicate_examples))
    return {"row_count": rows, "years": sorted(years), "null_counts": nulls, "issues": issues,
            "observation_status": "incomplete" if issues else "complete"}


def _code_key(value, numeric: bool) -> str | None:
    if value is None:
        return None
    if numeric:
        try:
            number = Decimal(str(int(value) if isinstance(value, bool) else value))
            if not number.is_finite():
                return None
            return str(number.normalize())
        except (InvalidOperation, ValueError):
            return None
    return str(value)


def _declared_code_domains(record: dict) -> set[str]:
    """Use explicit IPEDS domain declarations, not the presence of sentinel labels."""
    domains = set()
    for field in ("format", "DataType"):
        value = str(record.get(field, "")).strip().lower()
        if value in {"cont", "continuous"}:
            domains.add("continuous")
        elif value in {"disc", "discrete", "categorical", "category"}:
            domains.add("categorical")
    return domains


def _component_code_domains(variable: dict, records: list[dict], year: int, numeric: bool) -> list[dict]:
    """Retain each source component's domain without inferring cell origins."""
    definitions = {}
    for definition in variable.get("source_metadata", []) or variable.get("imputation_parent_metadata", []):
        identity = _definition_identity(definition)
        if identity[0] == year:
            definitions[(*identity, _number(definition.get("varnumber")))] = definition
    if len(definitions) < 2:
        return []
    domains = []
    for definition in definitions.values():
        values = set()
        for record in records:
            if not _same_scope(record, definition):
                continue
            name = _name(record.get("varname"))
            if name:
                matches = name == _name(definition.get("varname"))
            elif str(record.get("label_scope", "")).lower() == "imputation_variable":
                matches = bool(variable.get("imputation_parent_metadata"))
            else:
                number = _number(definition.get("varnumber"))
                matches = bool(number) and _number(record.get("varnumber")) == number
            code = _code_key(record.get("codevalue"), numeric) if matches else None
            if code is not None:
                values.add(code)
        domains.append({"source_file": _name(definition.get("source_file")),
                        "access_table_name": _source_table(definition),
                        "varname": _name(definition.get("varname")),
                        "varnumber": _number(definition.get("varnumber")),
                        "codes": sorted(values)})
    return domains


def validate_observed_codes(dataset, metadata: dict, filt=None, batch_rows: int = 100_000,
                            year_col: str = "year") -> dict:
    """Compare observations with safely resolved code records in their own year.

Only variables declared categorical or carrying code records are checked.
Explicit ``format=Cont`` numeric measures have an open domain: their codebook
can document special values without enumerating ordinary measurements.
Unknown values are retained in the data and reported; they are never recoded.
Different categorical domains among same-year source components are ambiguous
without cell-level source lineage, even when their union covers every value.
    """
    checks = {}
    issues = []
    domain_by_year = {}
    for variable in metadata["variables"]:
        name = variable["name"]
        if name not in dataset.schema.names or name == year_col:
            continue
        records = variable.get("resolved_value_label_records", [])
        categorical = bool(records or variable.get("value_label_records") or variable.get("imputation_parent_metadata")) or any(
            "categorical" in _declared_code_domains(row)
            for row in variable.get("source_metadata", [])
        )
        if not categorical:
            continue
        dtype = dataset.schema.field(name).type
        numeric = pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_boolean(dtype)
        allowed = {}
        declarations: dict[int, set[str]] = {}
        for record in variable.get("source_metadata", []):
            try:
                year = int(record["year"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            declarations.setdefault(year, set()).update(_declared_code_domains(record))
        for record in variable.get("imputation_parent_metadata", []):
            try:
                declarations.setdefault(int(record["year"]), set()).add("categorical")
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
        for record in records:
            try:
                year = int(record["year"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            code = _code_key(record.get("codevalue"), numeric)
            if code is not None:
                allowed.setdefault(year, set()).add(code)
        modes = {}
        for year in set(declarations) | set(allowed) | set(metadata.get("years", [])):
            declared = declarations.get(year, set())
            if len(declared) > 1:
                modes[year] = "ambiguous"
                issues.append(_issue("code_domain_ambiguous", f"{name} has conflicting continuous/categorical source declarations in {year}.", name, year=year))
            elif declared == {"continuous"} and numeric:
                modes[year] = "continuous"
            elif declared == {"continuous"}:
                modes[year] = "ambiguous"
                issues.append(_issue("continuous_code_type_mismatch", f"{name} is declared continuous in {year} but its export storage type is {dtype}.", name, year=year))
            else:
                modes[year] = "categorical"
                components = _component_code_domains(variable, records, year, numeric)
                if len({tuple(component["codes"]) for component in components}) > 1:
                    modes[year] = "ambiguous"
                    issues.append(_issue(
                        "categorical_source_domain_ambiguous",
                        f"{name} has different categorical code domains across source components in {year}. "
                        "A union of their codes cannot validate observations without cell-level source identity.",
                        name, year=year, component_domains=components,
                    ))
        domain_by_year[name] = {str(year): mode for year, mode in sorted(modes.items())}
        checks[name] = (numeric, allowed, modes)
    if not checks:
        return {"issues": [], "checked_variables": [], "unknown_code_count": 0}
    if year_col not in dataset.schema.names:
        return {"issues": [_issue("code_year_missing", "Categorical values cannot be checked without a reporting year.", year_col)],
                "checked_variables": sorted(checks), "unknown_code_count": 0}
    columns = [year_col] + list(checks)
    counts = {name: 0 for name in checks}
    special_counts = {name: 0 for name in checks}
    samples = {name: [] for name in checks}
    for batch in dataset.to_batches(columns=columns, filter=filt, batch_size=batch_rows):
        years = batch.column(0).to_pylist()
        for index, (name, (numeric, allowed, modes)) in enumerate(checks.items(), 1):
            for year, value in zip(years, batch.column(index).to_pylist()):
                if value is None:
                    continue
                numeric_year = _positive_integer(year)
                code = _code_key(value, numeric)
                mode = modes.get(numeric_year, "categorical")
                if mode == "ambiguous":
                    continue
                if mode == "continuous" and code is not None:
                    if code in allowed.get(numeric_year, set()):
                        special_counts[name] += 1
                    continue
                if numeric_year is None or code is None or code not in allowed.get(numeric_year, set()):
                    counts[name] += 1
                    if len(samples[name]) < 5:
                        samples[name].append({"year": numeric_year, "value": str(value)})
    for name, count in counts.items():
        if count:
            issues.append(_issue("unknown_observed_code", f"{count} nonmissing values of {name} have no safely resolved code definition for their reporting year.", name, count=count, examples=samples[name]))
    return {"issues": issues, "checked_variables": list(checks), "unknown_code_count": sum(counts.values()),
            "categorical_variables": [name for name, (_, _, modes) in checks.items() if "categorical" in modes.values()],
            "continuous_variables": [name for name, (_, _, modes) in checks.items() if "continuous" in modes.values()],
            "special_code_count": sum(special_counts.values()), "special_code_counts": special_counts,
            "code_domain_by_year": domain_by_year}


def apply_observation_validation(metadata: dict, scan: dict, code_check: dict) -> None:
    issues = scan["issues"] + code_check["issues"]
    metadata["issues"].extend(issues)
    metadata["observation_validation"] = {
        "status": "incomplete" if issues else "complete", "issues": issues,
        "checked_categorical_variables": code_check.get("categorical_variables", code_check["checked_variables"]),
        "checked_continuous_variables": code_check.get("continuous_variables", []),
        "special_code_count": code_check.get("special_code_count", 0),
        "special_code_counts": code_check.get("special_code_counts", {}),
        "code_domain_by_year": code_check.get("code_domain_by_year", {}),
        "unknown_code_count": code_check["unknown_code_count"],
    }
    metadata["readiness_status"] = "complete" if metadata["metadata_status"] == "complete" and not issues else "incomplete"


def require_export_ready(metadata: dict) -> None:
    if metadata.get("readiness_status", metadata["metadata_status"]) != "complete":
        detail = "; ".join(issue["message"] for issue in metadata["issues"][:8])
        raise ValueError(f"Metadata is incomplete or observations are not ready: {detail}")


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_fingerprint(path: Path) -> dict:
    """Hash source bytes; directory identity is a sorted relative-file manifest."""
    path = Path(path)
    is_directory = path.is_dir()
    files = [Path(name) for name in ds.dataset(path, format="parquet").files] if is_directory else [path]
    records = []
    for file in sorted(files, key=lambda item: str(item)):
        before = file.stat()
        digest = _file_hash(file)
        after = file.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise ValueError(f"Source changed while hashing: {file}")
        records.append({"path": str(file.relative_to(path)) if is_directory else file.name,
                        "size_bytes": after.st_size, "sha256": digest})
    aggregate = hashlib.sha256(json.dumps(records, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"kind": "parquet_dataset" if is_directory else "file", "sha256": aggregate if is_directory else records[0]["sha256"], "files": records}


def assert_source_unchanged(path: Path, baseline: dict) -> None:
    if source_fingerprint(path) != baseline:
        raise ValueError(f"Source changed during export: {path}; no output package was promoted.")


def export_code_provenance() -> dict:
    scripts = Path(__file__).resolve().parent
    paths = [scripts / name for name in ("00_run_all.py", "08_build_custom_panel.py", "09_build_panel_dictionary.py", "panel_export.py", "export_metadata.py", "export_integrity.py", "run_saved_query.py")]
    result = {"files": {str(path.relative_to(scripts.parent)): _file_hash(path) for path in paths if path.is_file()},
              "python_version": platform.python_version(),
              "package_versions": {name: importlib.metadata.version(name) for name in ("pandas", "pyarrow", "openpyxl", "duckdb")}}
    try:
        result["git_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=scripts.parent, text=True, stderr=subprocess.DEVNULL, timeout=5).strip()
    except (OSError, subprocess.SubprocessError):
        result["git_commit"] = None
    return result


def promote_package(staged_data: Path, output: Path) -> None:
    """Promote five files, restoring prior contents if any replacement fails.

Backups are prepared before the first mutation and retained on rollback failure.
This does not promise crash atomicity or concurrent-reader isolation.
    """
    staged_data, output = Path(staged_data), Path(output)
    pairs = [(Path(str(staged_data) + suffix), Path(str(output) + suffix)) for suffix in PACKAGE_SUFFIXES]
    pairs.append((staged_data, output))
    for source, destination in pairs:
        if not source.is_file():
            raise ValueError(f"Incomplete staged export package: {source}")
        if destination.is_symlink() or (destination.exists() and not destination.is_file()):
            raise ValueError(f"Export destination is not a regular file: {destination}")
    backup = Path(tempfile.mkdtemp(prefix=".ipeds-export-backup-", dir=output.parent))
    prior = {}
    promoted = []
    cleanup = True
    try:
        for _, destination in pairs:
            if destination.exists():
                saved = backup / destination.name
                shutil.copy2(destination, saved)
                prior[destination] = saved
        try:
            for source, destination in pairs:
                os.replace(source, destination)
                promoted.append(destination)
        except BaseException as exc:
            failures = []
            for destination in reversed(promoted):
                try:
                    if destination in prior:
                        os.replace(prior[destination], destination)
                    else:
                        destination.unlink()
                except OSError as restore_exc:
                    failures.append(f"{destination}: {restore_exc}")
            if failures:
                cleanup = False
                raise RuntimeError(f"Export promotion and rollback failed; prior files retained at {backup}: {'; '.join(failures)}") from exc
            raise
    finally:
        if cleanup:
            shutil.rmtree(backup)
