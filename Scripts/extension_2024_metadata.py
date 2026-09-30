"""Attach the locked physical-table release provenance to 2024 metadata."""
from pathlib import Path

import pandas as pd


def attach_2024_provenance(frame: pd.DataFrame, *, root: Path) -> pd.DataFrame:
    if frame.empty or not pd.to_numeric(frame["year"]).eq(2024).any():
        return frame
    inventory = pd.read_csv(
        Path(root) / "Raw_Access_Databases/2024/metadata/table_inventory.csv",
        dtype=str, keep_default_na=False,
    )
    columns = ["source_release_type", "source_release_date", "source_url",
               "source_archive_sha256", "source_member_sha256"]
    missing = set(columns) - set(inventory)
    if missing:
        raise ValueError(f"2024 source inventory lacks release provenance: {sorted(missing)}")
    inventory["table_key"] = inventory["table_name"].str.upper().str.strip()
    if inventory.table_key.duplicated().any():
        raise ValueError("2024 source inventory repeats a physical table")
    lookup = inventory.set_index("table_key")
    result = frame.copy()
    selected = pd.to_numeric(result["year"]).eq(2024)
    keys = result["access_table_name"].astype(str).str.upper().str.strip()
    physical = selected & ~keys.eq("KEYS")
    missing_tables = sorted(set(keys[physical]) - set(lookup.index))
    if missing_tables:
        raise ValueError(f"2024 dictionary refers to absent physical tables: {missing_tables}")
    for column in columns:
        result.loc[physical, column] = keys[physical].map(lookup[column])
    if result.loc[physical, "source_release_type"].eq("").any():
        raise ValueError("2024 physical-table release status cannot be blank")
    result.loc[selected, "original_release_type"] = result.loc[selected, "release_type"]
    result.loc[physical, "release_type"] = result.loc[physical, "source_release_type"]
    documents = [path for path in (Path(root) / "Raw_Access_Databases/2024/tables_csv").glob("*.csv")
                 if path.stem.lower() == "tables24"]
    if len(documents) != 1:
        raise ValueError("2024 source requires exactly one original tables24 reference-period table")
    tables = pd.read_csv(documents[0], dtype=str, keep_default_na=False)
    tables["table_key"] = tables["TableName"].str.upper().str.strip()
    if tables.table_key.duplicated().any():
        raise ValueError("2024 tables24 repeats a physical table")
    periods = tables.set_index("table_key")["YearCoverage"]
    result.loc[physical, "source_table_reference_period"] = keys[physical].map(periods).fillna("")
    # YearCoverage describes a table broadly; it must not be promoted to an
    # identical measurement period/population for every variable in the table.
    if "varname" in result:
        pell = physical & result["varname"].str.upper().isin(["UPGRNTN", "UPGRNTT"])
        result.loc[pell, "reporting_population_note"] = (
            "IPEDS collection year 2024: academic reporters use the fall 2023 undergraduate cohort "
            "and institution-defined academic year 2023-24; program reporters use their documented "
            "reporting period. The table's July 2023-June 2024 coverage is not a universal student "
            "cohort. Do not assume equality with annual FSA recipients or disbursements."
        )
        result.loc[pell, "reporting_population_source"] = (
            "https://nces.ed.gov/ipeds/use-the-data/download-survey-material/2024/"
            "student%20financial%20aid/package_7_16.pdf"
        )
    return result
