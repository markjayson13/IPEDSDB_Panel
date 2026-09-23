#!/usr/bin/env python3
"""
Stage 08: build a custom variable subset from a raw or cleaned wide panel.

Reads:
- a stitched wide or cleaned wide parquet panel

Writes:
- a custom Parquet, CSV, Stata, or Excel extract with companion metadata

`UNITID` and `year` are always retained. Users choose the remaining variables
with `--vars` or `--vars-file`.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from access_build_utils import DEFAULT_IPEDSDB_ROOT
from export_metadata import build_export_metadata
from panel_export import prepare_format_metadata, sha256_file, write_excel, write_sidecars, write_stata, write_stream


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    f = log_file.open("a", buffering=1)

    class Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, data):
            for s in self.streams:
                s.write(data)

        def flush(self):
            for s in self.streams:
                s.flush()

    sys.stdout = Tee(sys.stdout, f)
    sys.stderr = Tee(sys.stderr, f)


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":")
        years = list(range(int(start), int(end) + 1))
    else:
        years = [int(x.strip()) for x in spec.split(",") if x.strip()]
    if not years:
        raise ValueError("Year filter must contain at least one year in ascending range order.")
    return years


def discover_metadata(explicit: str | None, relative: str, input_path: Path, root: Path) -> Path | None:
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ValueError(f"Metadata file does not exist: {path}")
        return path
    # Prefer the input's own data root over an unrelated IPEDSDB_ROOT.
    for base in (input_path.parent.parent, root):
        candidate = base / relative
        if candidate.is_file():
            return candidate
    return None


def load_vars(vars_arg: str | None, vars_file: str | None) -> list[str]:
    out: list[str] = []
    if vars_arg:
        out.extend([v.strip() for v in vars_arg.split(",") if v.strip()])
    if vars_file:
        p = Path(vars_file)
        if not p.exists():
            raise FileNotFoundError(p)
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.extend([v.strip() for v in line.split(",") if v.strip()])
    # de-dup while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for v in out:
        key = v.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="Input wide parquet (raw/clean)")
    ap.add_argument("--output", required=True, help="Output .parquet, .csv, .dta, or .xlsx path")
    ap.add_argument("--vars", default=None, help="Comma-separated list of varnames")
    ap.add_argument("--vars-file", default=None, help="File with varnames (one per line or comma-separated)")
    ap.add_argument("--years", default=None, help='Optional year filter, e.g. "2004:2023" or "2004,2006"')
    ap.add_argument("--format", choices=["parquet", "csv", "dta", "xlsx"], default=None, help="Default: infer from output extension")
    ap.add_argument("--dictionary", help="dictionary_lake.parquet; auto-discovered beside the input data root")
    ap.add_argument("--codes", help="dictionary_codes.parquet; auto-discovered beside the dictionary")
    ap.add_argument("--column-lineage", help="Stage 06 qc_column_lineage.csv/parquet for collapsed/aliased columns")
    ap.add_argument("--require-metadata", action="store_true", help="Fail if source definitions or code metadata are missing/ambiguous")
    ap.add_argument("--batch-rows", type=int, default=100_000, help="Batch size for streaming output")
    ap.add_argument("--strict", action="store_true", help="Fail if any requested varname is missing")
    data_root = Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT)))
    ap.add_argument("--log-file", default=str(data_root / "Checks" / "logs" / "08_build_custom_panel.log"), help="Optional log file path")
    args = ap.parse_args()
    setup_logging(args.log_file)
    if args.batch_rows < 1:
        raise ValueError("--batch-rows must be positive.")
    input_path = Path(args.input)
    out_path = Path(args.output)
    suffix_format = out_path.suffix.lower().lstrip(".")
    fmt = args.format or suffix_format
    if fmt not in {"parquet", "csv", "dta", "xlsx"}:
        raise ValueError("Use an output extension .parquet, .csv, .dta, or .xlsx, or specify --format.")
    if suffix_format in {"parquet", "csv", "dta", "xlsx"} and fmt != suffix_format:
        raise ValueError("--format must match the output extension.")
    if out_path.resolve() == input_path.resolve() or (input_path.is_dir() and input_path.resolve() in out_path.resolve().parents):
        raise ValueError("Output must not overwrite or become part of the source dataset.")

    vars_requested = load_vars(args.vars, args.vars_file)
    if not vars_requested:
        raise SystemExit("Provide --vars or --vars-file with at least one variable.")

    dataset = ds.dataset(args.input, format="parquet")
    schema = dataset.schema
    if len({name.upper() for name in schema.names}) != len(schema.names):
        raise ValueError("Input has duplicate or case-ambiguous column names.")

    # Resolve column names case-insensitively.
    name_map = {name.upper(): name for name in schema.names}
    year_col = name_map.get("YEAR", "year" if "year" in schema.names else None)
    unitid_col = name_map.get("UNITID", "UNITID" if "UNITID" in schema.names else None)
    if not year_col or not unitid_col:
        raise SystemExit("Input must include UNITID and year columns.")

    requested_upper = [v.upper() for v in vars_requested]
    resolved = []
    missing = []
    for v in requested_upper:
        if v in name_map:
            resolved.append(name_map[v])
        else:
            missing.append(v)
    if missing:
        msg = f"Missing {len(missing)} vars: {', '.join(missing[:20])}"
        if args.strict:
            raise SystemExit(msg)
        print("[warn]", msg)

    cols = [year_col, unitid_col] + resolved
    # De-dup while preserving order
    seen: set[str] = set()
    cols = [c for c in cols if not (c in seen or seen.add(c))]

    filt = None
    if args.years:
        years = parse_years(args.years)
        if len(years) == 1:
            filt = ds.field(year_col) == years[0]
        elif ":" in args.years:
            start, end = years[0], years[-1]
            filt = (ds.field(year_col) >= start) & (ds.field(year_col) <= end)
        else:
            filt = ds.field(year_col).isin(years)

    embedded = any((schema.field(name).metadata or {}).get(b"ipeds:variable") for name in cols)
    use_embedded = embedded and not args.dictionary and not args.codes
    dictionary = None if use_embedded else discover_metadata(args.dictionary, "Dictionary/dictionary_lake.parquet", input_path, data_root)
    if args.codes:
        codes = discover_metadata(args.codes, "Dictionary/dictionary_codes.parquet", input_path, data_root)
    elif dictionary:
        # A separately supplied dictionary must not silently borrow category
        # codes from a different release/data root.
        sibling_codes = dictionary.parent / "dictionary_codes.parquet"
        codes = sibling_codes if sibling_codes.is_file() else None
    elif use_embedded:
        codes = None
    else:
        codes = discover_metadata(None, "Dictionary/dictionary_codes.parquet", input_path, data_root)
    lineage = None if use_embedded and not args.column_lineage else discover_metadata(args.column_lineage, "Checks/wide_qc/qc_column_lineage.csv", input_path, data_root)
    if any(p and p.resolve() == out_path.resolve() for p in (dictionary, codes, lineage)):
        raise ValueError("Output cannot overwrite source metadata.")
    selected_schema = pa.schema([schema.field(c) for c in cols], metadata=schema.metadata)
    actual_years = set()
    null_counts = {c: 0 for c in cols}
    row_count = 0
    # Bounded memory pass supplies actual coverage and missingness, including empty extracts.
    for batch in dataset.to_batches(columns=cols, filter=filt, batch_size=args.batch_rows):
        row_count += batch.num_rows
        for c, values in zip(cols, batch.columns):
            null_counts[c] += values.null_count
        for value in batch.column(cols.index(year_col)).to_pylist():
            if value is None or isinstance(value, bool) or int(value) != value:
                raise ValueError("The reporting year must be a nonmissing integer.")
            actual_years.add(int(value))
    metadata = build_export_metadata(selected_schema, sorted(actual_years), dictionary, codes, lineage)
    metadata["metadata_source_modes"] = metadata.get("metadata_sources", {})
    metadata.update({
        "schema_version": "1.0", "created_utc": datetime.now(timezone.utc).isoformat(),
        "source_panel": str(input_path.resolve()), "format": fmt, "row_count": row_count,
        "column_count": len(cols), "panel_keys": [unitid_col, year_col],
        "requested_years": parse_years(args.years) if args.years else None,
        "missing_requested_variables": missing,
        "metadata_sources": {k: str(p.resolve()) if p else None for k, p in
                             (("dictionary", dictionary), ("codes", codes), ("lineage", lineage))},
        "metadata_source_sha256": {k: sha256_file(p) if p else None for k, p in
                                  (("dictionary", dictionary), ("codes", codes), ("lineage", lineage))},
        "missing_values": "Input nulls remain missing. Observed negative/special codes remain unchanged. "
                          "Stata uses numeric system missing and empty strings; CSV uses unquoted empty fields; "
                          "Excel uses blank cells. No missing-reason codes are invented.",
    })
    for var in metadata["variables"]:
        var["null_count"] = null_counts[var["name"]]
    prepare_format_metadata(metadata, selected_schema, fmt)
    if args.require_metadata and metadata["metadata_status"] != "complete":
        detail = "; ".join(issue["message"] for issue in metadata["issues"][:8])
        raise ValueError(f"Metadata is incomplete: {detail}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Validate/write a whole package before replacing any existing destination files.
    with tempfile.TemporaryDirectory(prefix=".ipeds-export-", dir=out_path.parent) as staging:
        temp_path = Path(staging) / out_path.name
        if fmt in {"parquet", "csv"}:
            rows = write_stream(dataset, cols, filt, args.batch_rows, temp_path, fmt, selected_schema, metadata)
        elif fmt == "xlsx":
            rows = write_excel(dataset, cols, filt, args.batch_rows, temp_path, metadata)
        else:
            table = dataset.to_table(columns=cols, filter=filt)
            write_stata(table, temp_path, metadata)
            rows = table.num_rows
        if rows != row_count:
            raise ValueError("Source row count changed during export; rerun against a stable input.")
        write_sidecars(temp_path, metadata)
        for sidecar in sorted(Path(staging).iterdir()):
            if sidecar != temp_path:
                os.replace(sidecar, out_path.parent / sidecar.name)
        os.replace(temp_path, out_path)
    print(f"Wrote {out_path} rows={rows:,} cols={len(cols)} metadata={metadata['metadata_status']}")
    if metadata["issues"]:
        print(f"[warn] {len(metadata['issues'])} metadata/format issues recorded in {out_path.name}.metadata.json")


if __name__ == "__main__":
    main()
