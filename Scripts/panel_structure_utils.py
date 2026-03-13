#!/usr/bin/env python3
"""
Shared panel-structure diagnostics used by literature-guided QA scripts.

Reads:
- panel columns supplied by callers, usually from the cleaned wide panel
- optional identifier and classification columns such as `OPEID`, `CONTROL`,
  `SECTOR`, `ICLEVEL`, `LEVEL`, `CARNEGIE`, `C18BASIC`, and `C21BASIC`

Writes:
- no project artifacts directly

Focus:
- classify institution presence patterns in an unbalanced panel
- summarize identifier linkage without changing the canonical UNITID panel
- provide readable diagnostics that match the repo's QA outputs

Open this file when you want the reusable logic behind the repo's panel-structure, linkage, and comparability summaries.

Method context:
- `METHODS_PANEL_CONSTRUCTION.md` explains why these diagnostics exist
- this module keeps those definitions stable across QA scripts and tests
"""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


CLASSIFICATION_COLUMNS = [
    "CONTROL",
    "SECTOR",
    "ICLEVEL",
    "LEVEL",
    "CARNEGIE",
    "C18BASIC",
    "C21BASIC",
]

FINANCE_KEYWORD_RULES = {
    "accounting_standard_sensitive": (
        "depreciation",
        "interest",
        "maintenance",
        "operation and maintenance",
    ),
    "balance_sheet_parent_child_sensitive": (
        "asset",
        "liabilit",
    ),
}


def normalized_nonnull_tokens(values: Iterable[object]) -> list[str]:
    out: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "<na>", "nat"}:
            continue
        out.append(text)
    return sorted(set(out))


def best_mode_text(series: pd.Series) -> str:
    values = normalized_nonnull_tokens(series.tolist())
    if not values:
        return ""
    counts = series.dropna().astype(str).str.strip()
    counts = counts[counts != ""].value_counts()
    if counts.empty:
        return values[0]
    max_count = counts.max()
    top = sorted(val for val, count in counts.items() if count == max_count)
    return top[0]


def classify_presence_pattern(years_present: Iterable[int], panel_years: Iterable[int]) -> dict[str, object]:
    panel_years_sorted = sorted({int(year) for year in panel_years})
    if not panel_years_sorted:
        raise ValueError("panel_years must be non-empty")
    present_sorted = sorted({int(year) for year in years_present})
    if not present_sorted:
        raise ValueError("years_present must be non-empty")

    panel_start = panel_years_sorted[0]
    panel_end = panel_years_sorted[-1]
    first_year = present_sorted[0]
    last_year = present_sorted[-1]
    expected_span_years = last_year - first_year + 1
    missing_internal_years = [
        year for year in range(first_year, last_year + 1) if year not in set(present_sorted)
    ]
    entered_after_start = first_year > panel_start
    exited_before_end = last_year < panel_end
    intermittent_gap = bool(missing_internal_years)

    if not entered_after_start and not exited_before_end and not intermittent_gap:
        pattern = "always_present"
    elif intermittent_gap:
        pattern = "intermittent_gap"
    elif entered_after_start and exited_before_end:
        pattern = "entered_and_exited"
    elif entered_after_start:
        pattern = "entered_after_start"
    elif exited_before_end:
        pattern = "exited_before_end"
    else:
        pattern = "other"

    return {
        "panel_start_year": panel_start,
        "panel_end_year": panel_end,
        "first_year": first_year,
        "last_year": last_year,
        "observed_years": len(present_sorted),
        "expected_span_years": expected_span_years,
        "missing_internal_years": len(missing_internal_years),
        "missing_internal_year_list": ",".join(str(year) for year in missing_internal_years),
        "entered_after_start": bool(entered_after_start),
        "exited_before_end": bool(exited_before_end),
        "intermittent_gap": bool(intermittent_gap),
        "pattern": pattern,
        "likely_new_entry": bool(entered_after_start and not intermittent_gap),
        "likely_exit_or_closure": bool(exited_before_end and not intermittent_gap),
        "possible_selection_risk": bool(exited_before_end or intermittent_gap),
    }


def build_institution_pattern_flags(
    panel_df: pd.DataFrame,
    panel_years: Iterable[int],
    *,
    unitid_col: str = "UNITID",
    year_col: str = "year",
    opeid_col: str = "OPEID",
    instnm_col: str = "INSTNM",
    stabbr_col: str = "STABBR",
) -> pd.DataFrame:
    df = panel_df.copy()
    df = df.dropna(subset=[unitid_col, year_col]).copy()
    df[unitid_col] = pd.to_numeric(df[unitid_col], errors="coerce").astype("Int64")
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    df = df.dropna(subset=[unitid_col, year_col]).copy()
    df[unitid_col] = df[unitid_col].astype(int)
    df[year_col] = df[year_col].astype(int)

    rows: list[dict[str, object]] = []
    for unitid, frame in df.groupby(unitid_col):
        presence = classify_presence_pattern(frame[year_col].tolist(), panel_years)
        opeid_values = normalized_nonnull_tokens(frame[opeid_col].tolist()) if opeid_col in frame.columns else []
        rows.append(
            {
                unitid_col: int(unitid),
                "years_present_list": "|".join(str(year) for year in sorted(frame[year_col].unique())),
                "opeid_count": len(opeid_values),
                "stable_opeid": opeid_values[0] if len(opeid_values) == 1 else "",
                "possible_identifier_linkage_case": len(opeid_values) > 1,
                "instnm_example": best_mode_text(frame[instnm_col]) if instnm_col in frame.columns else "",
                "stabbr_mode": best_mode_text(frame[stabbr_col]) if stabbr_col in frame.columns else "",
                **presence,
            }
        )
    return pd.DataFrame(rows)


def build_panel_structure_summary(
    panel_df: pd.DataFrame,
    patterns_df: pd.DataFrame,
    panel_years: Iterable[int],
    *,
    unitid_col: str = "UNITID",
) -> pd.DataFrame:
    years_sorted = sorted({int(year) for year in panel_years})
    metrics = [
        ("year_start", years_sorted[0], "Requested panel start year."),
        ("year_end", years_sorted[-1], "Requested panel end year."),
        ("years_total", len(years_sorted), "Number of years in the analysis window."),
        ("institution_year_rows", len(panel_df), "Number of institution-year rows in the cleaned panel."),
        ("distinct_unitids", int(panel_df[unitid_col].dropna().nunique()), "Number of distinct UNITID values in the cleaned panel."),
        ("always_present_unitids", int((patterns_df["pattern"] == "always_present").sum()), "Institutions observed in every panel year without internal gaps."),
        ("entered_after_start_unitids", int((patterns_df["entered_after_start"]).sum()), "Institutions first observed after the panel start year."),
        ("exited_before_end_unitids", int((patterns_df["exited_before_end"]).sum()), "Institutions last observed before the panel end year."),
        ("entered_and_exited_unitids", int((patterns_df["pattern"] == "entered_and_exited").sum()), "Institutions that both entered after the start and exited before the end."),
        ("intermittent_gap_unitids", int((patterns_df["intermittent_gap"]).sum()), "Institutions with internal missing-year gaps between first and last observation."),
        ("possible_selection_risk_unitids", int((patterns_df["possible_selection_risk"]).sum()), "Institutions flagged for potential attrition or intermittent-reporting selection risk."),
        ("possible_identifier_linkage_cases", int((patterns_df["possible_identifier_linkage_case"]).sum()), "Institutions with more than one observed OPEID-like identifier over time."),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value", "notes"])


def build_entry_exit_gap_summary(
    panel_df: pd.DataFrame,
    patterns_df: pd.DataFrame,
    panel_years: Iterable[int],
    *,
    unitid_col: str = "UNITID",
    year_col: str = "year",
) -> pd.DataFrame:
    years_sorted = sorted({int(year) for year in panel_years})
    rows: list[dict[str, object]] = []

    panel_df = panel_df[[unitid_col, year_col]].dropna().copy()
    panel_df[unitid_col] = pd.to_numeric(panel_df[unitid_col], errors="coerce").astype("Int64")
    panel_df[year_col] = pd.to_numeric(panel_df[year_col], errors="coerce").astype("Int64")
    panel_df = panel_df.dropna().copy()
    panel_df[unitid_col] = panel_df[unitid_col].astype(int)
    panel_df[year_col] = panel_df[year_col].astype(int)

    year_sets = {
        year: set(panel_df.loc[panel_df[year_col] == year, unitid_col].unique())
        for year in years_sorted
    }

    for year in years_sorted:
        rows.append(
            {
                "record_type": "year_status",
                "year": year,
                "from_year": pd.NA,
                "to_year": pd.NA,
                "pattern": "",
                "institution_count": pd.NA,
                "unitids_present": len(year_sets[year]),
                "entrants": int((patterns_df["first_year"] == year).sum()),
                "exits_after_year": int((patterns_df["last_year"] == year).sum()) if year < years_sorted[-1] else 0,
                "intermittent_unitids_present": int(
                    patterns_df.loc[
                        patterns_df["intermittent_gap"] & patterns_df["years_present_list"].str.contains(fr"(?:^|\|){year}(?:\||$)", regex=True),
                        unitid_col,
                    ].nunique()
                ),
                "retained_to_next_year": pd.NA,
                "retention_rate": pd.NA,
                "notes": "Counts of institutions present, entering, exiting, or intermittently observed in this panel year.",
            }
        )

    for idx in range(len(years_sorted) - 1):
        from_year = years_sorted[idx]
        to_year = years_sorted[idx + 1]
        from_set = year_sets[from_year]
        to_set = year_sets[to_year]
        retained = len(from_set & to_set)
        rows.append(
            {
                "record_type": "retention",
                "year": pd.NA,
                "from_year": from_year,
                "to_year": to_year,
                "pattern": "",
                "institution_count": pd.NA,
                "unitids_present": len(from_set),
                "entrants": pd.NA,
                "exits_after_year": pd.NA,
                "intermittent_unitids_present": pd.NA,
                "retained_to_next_year": retained,
                "retention_rate": float(retained / len(from_set)) if from_set else pd.NA,
                "notes": "Year-to-year retention of UNITID values in the unbalanced panel.",
            }
        )

    for pattern, count in patterns_df["pattern"].value_counts().sort_index().items():
        rows.append(
            {
                "record_type": "pattern_count",
                "year": pd.NA,
                "from_year": pd.NA,
                "to_year": pd.NA,
                "pattern": pattern,
                "institution_count": int(count),
                "unitids_present": pd.NA,
                "entrants": pd.NA,
                "exits_after_year": pd.NA,
                "intermittent_unitids_present": pd.NA,
                "retained_to_next_year": pd.NA,
                "retention_rate": pd.NA,
                "notes": "Overall institution pattern counts across the full analysis window.",
            }
        )
    return pd.DataFrame(rows)


def build_identifier_linkage_summary(
    panel_df: pd.DataFrame,
    panel_years: Iterable[int],
    *,
    unitid_col: str = "UNITID",
    year_col: str = "year",
    opeid_col: str = "OPEID",
) -> pd.DataFrame:
    years_sorted = sorted({int(year) for year in panel_years})
    if opeid_col not in panel_df.columns:
        return pd.DataFrame(
            [
                {
                    "record_type": "unavailable",
                    "year": pd.NA,
                    "distinct_unitids": pd.NA,
                    "distinct_opeid": pd.NA,
                    "rows_missing_opeid": pd.NA,
                    "shared_opeid_groups": pd.NA,
                    "unitids_in_shared_opeids": pd.NA,
                    "unitids_with_multiple_opeid": pd.NA,
                    "opeids_with_multiple_unitids_any_year": pd.NA,
                    "notes": "No OPEID-like column was available in the panel, so linkage diagnostics were skipped gracefully.",
                }
            ]
        )

    df = panel_df[[year_col, unitid_col, opeid_col]].copy()
    df[unitid_col] = pd.to_numeric(df[unitid_col], errors="coerce").astype("Int64")
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    df[opeid_col] = df[opeid_col].fillna("").astype(str).str.strip()
    df = df.dropna(subset=[unitid_col, year_col]).copy()
    df[unitid_col] = df[unitid_col].astype(int)
    df[year_col] = df[year_col].astype(int)

    rows: list[dict[str, object]] = []
    for year in years_sorted:
        frame = df.loc[df[year_col] == year].copy()
        nonnull = frame.loc[frame[opeid_col] != ""].copy()
        if nonnull.empty:
            shared_groups = 0
            shared_unitids = 0
            distinct_opeid = 0
        else:
            by_opeid = nonnull.groupby(opeid_col)[unitid_col].nunique()
            shared = by_opeid[by_opeid > 1]
            shared_groups = int(len(shared))
            shared_unitids = int(shared.sum())
            distinct_opeid = int(nonnull[opeid_col].nunique())
        rows.append(
            {
                "record_type": "year_summary",
                "year": year,
                "distinct_unitids": int(frame[unitid_col].nunique()),
                "distinct_opeid": distinct_opeid,
                "rows_missing_opeid": int((frame[opeid_col] == "").sum()),
                "shared_opeid_groups": shared_groups,
                "unitids_in_shared_opeids": shared_unitids,
                "unitids_with_multiple_opeid": pd.NA,
                "opeids_with_multiple_unitids_any_year": pd.NA,
                "notes": "Within-year linkage counts for UNITID and OPEID-like identifiers.",
            }
        )

    nonnull_all = df.loc[df[opeid_col] != ""].copy()
    if nonnull_all.empty:
        multi_opeid = 0
        opeid_multi_unitid = 0
    else:
        multi_opeid = int((nonnull_all.groupby(unitid_col)[opeid_col].nunique() > 1).sum())
        opeid_multi_unitid = int((nonnull_all.groupby(opeid_col)[unitid_col].nunique() > 1).sum())
    rows.append(
        {
            "record_type": "overall",
            "year": pd.NA,
            "distinct_unitids": int(df[unitid_col].nunique()),
            "distinct_opeid": int(nonnull_all[opeid_col].nunique()) if not nonnull_all.empty else 0,
            "rows_missing_opeid": int((df[opeid_col] == "").sum()),
            "shared_opeid_groups": pd.NA,
            "unitids_in_shared_opeids": pd.NA,
            "unitids_with_multiple_opeid": multi_opeid,
            "opeids_with_multiple_unitids_any_year": opeid_multi_unitid,
            "notes": "Overall identifier-linkage diagnostics. These are flags for review, not canonical rewrites.",
        }
    )
    return pd.DataFrame(rows)


def build_classification_stability_summary(
    panel_df: pd.DataFrame,
    classification_cols: Iterable[str] | None = None,
    *,
    unitid_col: str = "UNITID",
    year_col: str = "year",
) -> pd.DataFrame:
    cols = [col for col in (classification_cols or CLASSIFICATION_COLUMNS) if col in panel_df.columns]
    if not cols:
        return pd.DataFrame(
            [
                {
                    "record_type": "unavailable",
                    "variable": "",
                    "year": pd.NA,
                    "value": "",
                    "institution_count": pd.NA,
                    "units_observed": pd.NA,
                    "units_changed": pd.NA,
                    "change_rate": pd.NA,
                    "notes": "No configured classification columns were available in the panel.",
                }
            ]
        )

    df = panel_df[[unitid_col, year_col] + cols].copy()
    df[unitid_col] = pd.to_numeric(df[unitid_col], errors="coerce").astype("Int64")
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    df = df.dropna(subset=[unitid_col, year_col]).copy()
    df[unitid_col] = df[unitid_col].astype(int)
    df[year_col] = df[year_col].astype(int)

    rows: list[dict[str, object]] = []
    for variable in cols:
        frame = df[[unitid_col, year_col, variable]].copy()
        frame[variable] = frame[variable].fillna("").astype(str).str.strip()

        for (year, value), group in frame.loc[frame[variable] != ""].groupby([year_col, variable]):
            rows.append(
                {
                    "record_type": "year_frequency",
                    "variable": variable,
                    "year": int(year),
                    "value": value,
                    "institution_count": int(group[unitid_col].nunique()),
                    "units_observed": pd.NA,
                    "units_changed": pd.NA,
                    "change_rate": pd.NA,
                    "notes": "Annual frequency distribution for a longitudinally unstable classification field.",
                }
            )

        unit_counts = frame.loc[frame[variable] != ""].groupby(unitid_col)[variable].nunique()
        units_observed = int(len(unit_counts))
        units_changed = int((unit_counts > 1).sum())
        rows.append(
            {
                "record_type": "institution_change",
                "variable": variable,
                "year": pd.NA,
                "value": "",
                "institution_count": pd.NA,
                "units_observed": units_observed,
                "units_changed": units_changed,
                "change_rate": float(units_changed / units_observed) if units_observed else pd.NA,
                "notes": "Share of institutions whose observed classification changed across years.",
            }
        )
    return pd.DataFrame(rows)


def component_timing_reference_rows() -> list[dict[str, str]]:
    return [
        {
            "source_group": "HD, IC, ADM, AL",
            "representative_sources": "HD, IC, ADM, AL",
            "panel_year_interpretation": "Collection-year institutional or fall-cycle context",
            "timing_note": "These component families generally describe institutional characteristics or current-cycle context for the panel year rather than a prior cohort history.",
            "comparability_caution": "Definitions and category cut points can change over time; treat longitudinal shifts in classification variables as real diagnostics, not stable attributes by default.",
            "literature_basis": "NCES Access docs, DLDT modules, Institutional Groupings guidance",
        },
        {
            "source_group": "Enrollment and activity",
            "representative_sources": "EFA, EFB, EFC, EFCP, EFFY, EFIA, EAP",
            "panel_year_interpretation": "Collection-year enrollment snapshot or 12-month activity tied to the panel year",
            "timing_note": "Some enrollment measures are fall snapshots while others summarize 12-month activity. The panel year is the collection-year start, not a universal event date for all enrollment variables.",
            "comparability_caution": "Do not assume all EF-family measures refer to the same within-year reference period.",
            "literature_basis": "Jaquette & Parra (2014), NCES methodology and DLDT materials",
        },
        {
            "source_group": "Completions",
            "representative_sources": "C_A, C_B, C_C, CDEP, DRVC",
            "panel_year_interpretation": "Awards granted over the 12-month period ending in the collection cycle",
            "timing_note": "Completions align to an award period, not a same-day institutional snapshot.",
            "comparability_caution": "Parent-child reporting and derived completions variables should be handled component-specifically.",
            "literature_basis": "Jaquette & Parra (2014), repo PRCH method",
        },
        {
            "source_group": "Finance",
            "representative_sources": "F_F, F_FA, F_FA_F, F_FA_G, DRVF",
            "panel_year_interpretation": "Institution fiscal year reported in the collection cycle",
            "timing_note": "Finance variables often reflect fiscal-year reporting windows that differ across institutions.",
            "comparability_caution": "Accounting-standard-sensitive variables should be reviewed before cross-year comparison; no automatic DCP-style harmonization is applied in this repo.",
            "literature_basis": "DCP documentation, Cheslock & Jaquette (2016), Cheslock & Shamekhi (2020)",
        },
        {
            "source_group": "Graduation and outcomes",
            "representative_sources": "GR, GR200, GR_PELL_SSL, OM, DRVGR, DRVOM",
            "panel_year_interpretation": "Prior entering cohorts evaluated in the collection cycle",
            "timing_note": "These measures are cohort-based outcomes, not same-period outputs for the panel year.",
            "comparability_caution": "Interpret year labels as collection anchors for older cohorts rather than as contemporaneous outcomes.",
            "literature_basis": "Jaquette & Parra (2014), NCES methodology and survey docs",
        },
        {
            "source_group": "Human resources, salaries, and aid",
            "representative_sources": "S_*, SAL_*, SFA, SFA_P, SFAV",
            "panel_year_interpretation": "Collection-year workforce or aid reporting",
            "timing_note": "These variables often reflect current-cycle staffing or aid conditions rather than a harmonized academic-period measure.",
            "comparability_caution": "Definitions and subpopulations can shift over time; inspect documentation before treating categories as fixed.",
            "literature_basis": "NCES Access docs, Institutional Groupings and methodology guidance",
        },
    ]


def finance_comparability_flag(var_title: str, long_description: str) -> str:
    text = f"{var_title or ''} {long_description or ''}".lower()
    flags = [
        label
        for label, keywords in FINANCE_KEYWORD_RULES.items()
        if any(keyword in text for keyword in keywords)
    ]
    return "|".join(flags)
