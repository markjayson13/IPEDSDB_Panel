#!/usr/bin/env python3
"""
Stage 09: build a panel-specific variable dictionary for a stitched wide panel.

Reads:
- a stitched wide or cleaned wide parquet panel
- `Dictionary/dictionary_lake.parquet`

Writes:
- a panel-level dictionary keyed to the actual output columns
- `.csv` for plain export or `.xlsx` for a formatted workbook
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import pandas as pd
import pyarrow.dataset as ds

from export_metadata import build_export_metadata, discover_export_metadata
from export_integrity import (apply_observation_validation, assert_source_unchanged, export_code_provenance,
                              promote_package, require_export_ready, scan_panel, source_fingerprint, validate_observed_codes)
from panel_export import append_sheet, excel_cell, prepare_format_metadata, value_label_rows, write_sidecars
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Input stitched wide parquet")
    p.add_argument("--dictionary", required=True, help="dictionary_lake.parquet")
    p.add_argument("--output", required=True, help="Output .csv or .xlsx path")
    p.add_argument("--codes", help="Year/source-scoped category codebook; defaults beside the dictionary")
    p.add_argument("--column-lineage", help="Stage 06 output-column lineage; defaults beside the panel data root")
    p.add_argument("--require-ready", action="store_true", help="Require complete metadata and panel observation checks")
    return p.parse_args()


def reference_from_metadata(metadata: dict) -> pd.DataFrame:
    """Keep legacy dictionary columns without selecting a meaning across conflicting sources."""
    rows = []
    for index, variable in enumerate(metadata["variables"]):
        types = sorted({str(record.get("DataType", "")).strip()
                        for record in variable["source_metadata"] if record.get("DataType")})
        rows.append({
            "column_order": index, "varname": variable["name"].upper(),
            "varTitle": variable["label"], "longDescription": variable["description"],
            "panelDataType": variable["storage_type"],
            "dictionaryDataType": types[0] if len(types) == 1 else "",
            "metadata_status": variable["metadata_status"],
            "comparability_status": variable.get("comparability_status", "unknown"),
        })
    return pd.DataFrame(rows)


def build_reference_df(input_path: Path, dictionary_path: Path,
                       codes_path: Path | None = None, lineage_path: Path | None = None) -> pd.DataFrame:
    dataset = ds.dataset(input_path, format="parquet")
    names = {name.upper(): name for name in dataset.schema.names}
    scan = scan_panel(dataset, dataset.schema.names, panel_keys=(names.get("UNITID", "UNITID"), names.get("YEAR", "year")))
    metadata = build_export_metadata(dataset.schema, scan["years"], dictionary_path, codes_path, lineage_path)
    return reference_from_metadata(metadata)


def write_excel(ref: pd.DataFrame, input_path: Path, dictionary_path: Path, out_path: Path, metadata: dict) -> None:
    if len(ref) > 1048575:
        raise ValueError("Panel dictionary exceeds Excel's worksheet row limit; use CSV.")
    wb = Workbook()
    ws = wb.active
    ws.title = "panel_dictionary"
    headers = list(ref.columns)
    ws.append([excel_cell(ws, value) for value in headers])
    for row in ref.itertuples(index=False, name=None):
        ws.append([excel_cell(ws, value) for value in row])

    header_fill = PatternFill(fill_type="solid", fgColor="17324D")
    header_font = Font(color="FFFFFF", bold=True)
    subhead_fill = PatternFill(fill_type="solid", fgColor="E9F0F7")

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    ws.freeze_panes = "A2"
    ws.sheet_view.showGridLines = False
    ws.row_dimensions[1].height = 24

    width_map = {
        "A": 14,
        "B": 18,
        "C": 42,
        "D": 110,
        "E": 18,
        "F": 20,
    }
    for col_letter, width in width_map.items():
        ws.column_dimensions[col_letter].width = width

    wrap_cols = {3, 4, 5, 6}
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            if cell.column in wrap_cols:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
            else:
                cell.alignment = Alignment(vertical="top")

    table_ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
    table = Table(displayName="PanelDictionary", ref=table_ref)
    table.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium2",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )
    ws.add_table(table)

    about = wb.create_sheet("about")
    about["A1"] = "Panel dictionary export"
    about["A1"].font = Font(bold=True, size=14)
    about["A3"] = "What this workbook is"
    about["A3"].font = Font(bold=True)
    about["A3"].fill = subhead_fill
    about["A4"] = (
        "A formatted dictionary for the actual columns present in the panel output. "
        "Open the 'panel_dictionary' sheet for the variable list."
    )
    about["A6"] = "Source panel"
    about["A6"].font = Font(bold=True)
    about["B6"] = str(input_path)
    about["A7"] = "Source metadata"
    about["A7"].font = Font(bold=True)
    about["B7"] = str(dictionary_path)
    about["A8"] = "Rows in dictionary"
    about["A8"].font = Font(bold=True)
    about["B8"] = int(len(ref))
    about["A10"] = "Columns in main sheet"
    about["A10"].font = Font(bold=True)
    about["A11"] = "column_order"
    about["A12"] = "varname"
    about["A13"] = "varTitle"
    about["A14"] = "longDescription"
    about["A15"] = "panelDataType"
    about["A16"] = "dictionaryDataType"
    about.column_dimensions["A"].width = 26
    about.column_dimensions["B"].width = 120
    for row in about.iter_rows(min_row=1, max_row=16, min_col=1, max_col=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    for name, (sheet_headers, rows) in (
        ("value_labels", value_label_rows(metadata)),
        ("issues", (["severity", "code", "variable", "message"],
                    [[issue.get(key, "") for key in ("severity", "code", "variable", "message")]
                     for issue in metadata["issues"]])),
    ):
        append_sheet(wb, name, sheet_headers, rows)
    wb.save(out_path)
    wb.close()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    dictionary_path = Path(args.dictionary)
    codes_path = Path(args.codes) if args.codes else dictionary_path.parent / "dictionary_codes.parquet"
    lineage_path = discover_export_metadata(args.column_lineage, "Checks/wide_qc/qc_column_lineage.csv",
                                            input_path, input_path.parent.parent)
    for explicit, path in ((True, dictionary_path), (bool(args.codes), codes_path), (bool(args.column_lineage), lineage_path)):
        if explicit and (path is None or not path.is_file()):
            raise ValueError(f"Metadata file does not exist: {path}")
    codes_path = codes_path if codes_path.is_file() else None
    lineage_path = lineage_path if lineage_path and lineage_path.is_file() else None
    out_path = Path(args.output)
    if out_path.suffix.lower() not in {".csv", ".xlsx"}:
        raise ValueError("Panel dictionaries require a .csv or .xlsx extension.")
    sources = {"panel": input_path, "dictionary": dictionary_path, "codes": codes_path, "lineage": lineage_path}
    destinations = [out_path] + [Path(str(out_path) + suffix) for suffix in
                                (".metadata.json", ".dictionary.csv", ".value_labels.csv", ".README.txt")]
    if any(destination.resolve() == source.resolve() or (source.is_dir() and source.resolve() in destination.resolve().parents)
           for source in sources.values() if source for destination in destinations):
        raise ValueError("Dictionary output cannot overwrite source data or metadata.")
    fingerprints = {name: source_fingerprint(path) for name, path in sources.items() if path}
    dataset = ds.dataset(input_path, format="parquet")
    schema = dataset.schema
    names = {name.upper(): name for name in schema.names}
    if len(names) != len(schema.names):
        raise ValueError("Input has duplicate or case-ambiguous column names.")
    year_col = names.get("YEAR", "year")
    scan = scan_panel(dataset, schema.names, panel_keys=(names.get("UNITID", "UNITID"), year_col))
    metadata = build_export_metadata(schema, scan["years"], dictionary_path, codes_path, lineage_path)
    metadata.update({
        "schema_version": "1.1", "artifact_kind": "panel_dictionary",
        "created_utc": datetime.now(timezone.utc).isoformat(), "format": out_path.suffix[1:],
        "source_panel": str(input_path.resolve()), "source_row_count": scan["row_count"],
        "row_count": len(schema), "column_count": len(schema),
        "source_fingerprints": fingerprints, "export_code": export_code_provenance(),
        "missing_values": "Definitions describe source panel values. Null reasons and cross-year comparability are not inferred.",
    })
    for variable in metadata["variables"]:
        variable["null_count"] = scan["null_counts"][variable["name"]]
    prepare_format_metadata(metadata, schema, out_path.suffix[1:])
    code_check = validate_observed_codes(dataset, metadata, year_col=year_col)
    apply_observation_validation(metadata, scan, code_check)
    if args.require_ready:
        require_export_ready(metadata)
    ref = reference_from_metadata(metadata)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ipeds-dictionary-", dir=out_path.parent) as staging:
        staged = Path(staging) / out_path.name
        if out_path.suffix.lower() == ".xlsx":
            write_excel(ref, input_path, dictionary_path, staged, metadata)
        else:
            ref.to_csv(staged, index=False)
        write_sidecars(staged, metadata)
        for name, path in sources.items():
            if path:
                assert_source_unchanged(path, fingerprints[name])
        promote_package(staged, out_path)
    print(f"Wrote {len(ref):,} rows to {out_path}; metadata={metadata['metadata_status']}")


if __name__ == "__main__":
    main()
