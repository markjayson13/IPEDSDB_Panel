#!/usr/bin/env python3
"""
QA 09: build literature-guided panel-structure and comparability diagnostics.

Reads:
- cleaned wide panel under `Panels/`
- `Dictionary/dictionary_lake.parquet`

Writes:
- `Checks/panel_qc/panel_structure_summary.csv`
- `Checks/panel_qc/entry_exit_gap_summary.csv`
- `Checks/panel_qc/identifier_linkage_summary.csv`
- `Checks/panel_qc/component_timing_reference.csv`
- `Checks/panel_qc/finance_comparability_summary.csv`
- `Checks/panel_qc/classification_stability_summary.csv`
- `Checks/panel_qc/institution_pattern_flags.csv`

Focus:
- literature-guided diagnostics for unbalancedness and identifier continuity
- comparability notes without changing the canonical UNITID-year panel
- readable QA outputs that back the release status claims in the README

Open this file when you want the repo's practical answer to: "What kind of unbalanced panel did we actually build, how stable are the identifiers and classifications, and where should I be cautious about timing or finance comparability?"

Method context:
- `METHODS_PANEL_CONSTRUCTION.md` is the narrative explanation
- this script turns that explanation into concrete QA artifacts
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from access_build_utils import ensure_data_layout, parse_years
from panel_structure_utils import (
    CLASSIFICATION_COLUMNS,
    build_classification_stability_summary,
    build_entry_exit_gap_summary,
    build_identifier_linkage_summary,
    build_institution_pattern_flags,
    build_panel_structure_summary,
    component_timing_reference_rows,
    finance_comparability_flag,
)


def parse_args() -> argparse.Namespace:
    data_root = Path(os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"))
    checks_root = data_root / "Checks"
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=str(data_root), help="External IPEDSDB_ROOT")
    p.add_argument("--years", default="2004:2023", help='Expected year span, e.g. "2004:2023"')
    p.add_argument("--input", default=None, help="Optional cleaned panel override")
    p.add_argument("--dictionary", default=None, help="Optional dictionary_lake override")
    p.add_argument("--out-dir", default=str(checks_root / "panel_qc"), help="Panel QA output directory")
    return p.parse_args()


def write_csv(path: Path, df: pd.DataFrame, columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if columns is not None:
        for column in columns:
            if column not in df.columns:
                df[column] = pd.Series(dtype="object")
        df = df[columns]
    df.to_csv(path, index=False)


def infer_paths(root: Path, years: list[int], input_path: str | None, dict_path: str | None) -> tuple[Path, Path]:
    layout = ensure_data_layout(root)
    start, end = years[0], years[-1]
    clean_path = Path(input_path) if input_path else layout.panels / f"panel_clean_analysis_{start}_{end}.parquet"
    dictionary_path = Path(dict_path) if dict_path else layout.dictionary / "dictionary_lake.parquet"
    return clean_path, dictionary_path


def load_panel_subset(clean_path: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(clean_path), format="parquet")
    preferred_cols = [
        "year",
        "UNITID",
        "OPEID",
        "INSTNM",
        "STABBR",
        *CLASSIFICATION_COLUMNS,
    ]
    columns = [col for col in preferred_cols if col in dataset.schema.names]
    return dataset.to_table(columns=columns).to_pandas()


def build_finance_comparability_summary(clean_path: Path, dictionary_path: Path) -> pd.DataFrame:
    panel_columns = set(ds.dataset(str(clean_path), format="parquet").schema.names)
    dict_df = pd.read_parquet(
        dictionary_path,
        columns=["varname", "source_file", "varTitle", "longDescription"],
    )
    dict_df["varname"] = dict_df["varname"].fillna("").astype(str).str.upper().str.strip()
    dict_df["source_file"] = dict_df["source_file"].fillna("").astype(str).str.strip()
    dict_df = dict_df[dict_df["varname"].isin(panel_columns)].copy()
    dict_df = dict_df[dict_df["source_file"].str.startswith("F_")].copy()

    rows = [
        {
            "record_type": "global_note",
            "varname": "",
            "source_file": "F_*",
            "varTitle": "Finance comparability caution",
            "comparability_flag": "general_finance_caution",
            "affected_years": "2004:2023",
            "note": (
                "Finance variables may not be perfectly comparable across years because accounting treatment, "
                "institution fiscal-year timing, and parent-child reporting can differ. This repo does not apply "
                "automatic DCP-style finance harmonization."
            ),
            "literature_basis": "DCP documentation; Cheslock & Jaquette (2016); Cheslock & Shamekhi (2020)",
        },
        {
            "record_type": "global_note",
            "varname": "",
            "source_file": "F_*",
            "varTitle": "Accounting-standard-sensitive finance review",
            "comparability_flag": "accounting_standard_sensitive",
            "affected_years": "2004:2023",
            "note": (
                "Depreciation, interest, operation and maintenance, assets, and liabilities should be reviewed "
                "carefully before being treated as strictly comparable across long windows."
            ),
            "literature_basis": "DCP documentation; Cheslock & Shamekhi (2020)",
        },
    ]

    if dict_df.empty:
        rows.append(
            {
                "record_type": "unavailable",
                "varname": "",
                "source_file": "",
                "varTitle": "",
                "comparability_flag": "",
                "affected_years": "",
                "note": "No finance-source variables from dictionary_lake were matched to the current panel schema.",
                "literature_basis": "Repo diagnostic fallback",
            }
        )
        return pd.DataFrame(rows)

    dedup = (
        dict_df.groupby(["varname", "source_file"], as_index=False)
        .agg(
            varTitle=("varTitle", lambda s: sorted(set(str(v).strip() for v in s.dropna() if str(v).strip()), key=lambda x: (-len(x), x))[0] if any(str(v).strip() for v in s.dropna()) else ""),
            longDescription=("longDescription", lambda s: sorted(set(str(v).strip() for v in s.dropna() if str(v).strip()), key=lambda x: (-len(x), x))[0] if any(str(v).strip() for v in s.dropna()) else ""),
        )
    )

    for _, row in dedup.iterrows():
        flag = finance_comparability_flag(row["varTitle"], row["longDescription"])
        if not flag:
            continue
        rows.append(
            {
                "record_type": "variable_flag",
                "varname": row["varname"],
                "source_file": row["source_file"],
                "varTitle": row["varTitle"],
                "comparability_flag": flag,
                "affected_years": "2004:2023",
                "note": "Keyword-based comparability flag derived from the finance variable title/description in dictionary_lake.",
                "literature_basis": "DCP documentation; Cheslock & Shamekhi (2020); repo finance QA",
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    years = parse_years(args.years)
    out_dir = Path(args.out_dir)
    clean_path, dictionary_path = infer_paths(Path(args.root), years, args.input, args.dictionary)
    out_dir.mkdir(parents=True, exist_ok=True)

    panel_df = load_panel_subset(clean_path)
    patterns_df = build_institution_pattern_flags(panel_df, years)

    panel_structure_summary = build_panel_structure_summary(panel_df, patterns_df, years)
    entry_exit_gap_summary = build_entry_exit_gap_summary(panel_df, patterns_df, years)
    identifier_linkage_summary = build_identifier_linkage_summary(panel_df, years)
    classification_stability_summary = build_classification_stability_summary(panel_df)
    component_timing_reference = pd.DataFrame(component_timing_reference_rows())
    finance_comparability_summary = build_finance_comparability_summary(clean_path, dictionary_path)

    write_csv(out_dir / "institution_pattern_flags.csv", patterns_df)
    write_csv(out_dir / "panel_structure_summary.csv", panel_structure_summary, ["metric", "value", "notes"])
    write_csv(
        out_dir / "entry_exit_gap_summary.csv",
        entry_exit_gap_summary,
        [
            "record_type",
            "year",
            "from_year",
            "to_year",
            "pattern",
            "institution_count",
            "unitids_present",
            "entrants",
            "exits_after_year",
            "intermittent_unitids_present",
            "retained_to_next_year",
            "retention_rate",
            "notes",
        ],
    )
    write_csv(
        out_dir / "identifier_linkage_summary.csv",
        identifier_linkage_summary,
        [
            "record_type",
            "year",
            "distinct_unitids",
            "distinct_opeid",
            "rows_missing_opeid",
            "shared_opeid_groups",
            "unitids_in_shared_opeids",
            "unitids_with_multiple_opeid",
            "opeids_with_multiple_unitids_any_year",
            "notes",
        ],
    )
    write_csv(
        out_dir / "component_timing_reference.csv",
        component_timing_reference,
        [
            "source_group",
            "representative_sources",
            "panel_year_interpretation",
            "timing_note",
            "comparability_caution",
            "literature_basis",
        ],
    )
    write_csv(
        out_dir / "finance_comparability_summary.csv",
        finance_comparability_summary,
        [
            "record_type",
            "varname",
            "source_file",
            "varTitle",
            "comparability_flag",
            "affected_years",
            "note",
            "literature_basis",
        ],
    )
    write_csv(
        out_dir / "classification_stability_summary.csv",
        classification_stability_summary,
        [
            "record_type",
            "variable",
            "year",
            "value",
            "institution_count",
            "units_observed",
            "units_changed",
            "change_rate",
            "notes",
        ],
    )

    print(f"Wrote literature-guided panel diagnostics to {out_dir}")


if __name__ == "__main__":
    main()
