from pathlib import Path
import sys

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "Scripts"))
from extension_2024_null_columns import validate_retained_null_columns


def fixture(tmp_path):
    con = duckdb.connect()
    raw = pd.DataFrame([dict(year=2024, variable_id="nces:TEST:1:EMPTY",
                            access_table_name="TEST2024", source_file="TEST", varname="EMPTY",
                            source_csv_sha256="a" * 64, value_status="missing",
                            raw_value="", value=None) for _ in range(2)])
    mapped = pd.DataFrame([dict(year=2024, variable_id="nces:TEST:1:EMPTY",
                               analysis_column="EMPTY", transformation="identity",
                               normalized_value=None) for _ in range(2)])
    coverage = pd.DataFrame([dict(analysis_column="EMPTY", eligible_value_rows=2,
                                 non_null_value_rows=0)])
    for name, frame in [("release_scalar_raw", raw), ("release_direct_mapped", mapped),
                        ("release_canonical_column_coverage", coverage)]:
        con.register("source", frame)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM source")
        con.unregister("source")
    contract = tmp_path / "contract.csv"
    pd.DataFrame([dict(year=2024, access_table_name="TEST2024", source_file="TEST",
                       varname="EMPTY", variable_id="nces:TEST:1:EMPTY", analysis_column="EMPTY",
                       source_csv_sha256="a" * 64, source_rows=2,
                       reason="Two verified blank cells")]).to_csv(contract, index=False)
    return con, contract


def test_exact_blank_source_column_is_retained_with_qc(tmp_path):
    con, contract = fixture(tmp_path)
    assert validate_retained_null_columns(con, [2024], tmp_path, contract) == 1
    qc = pd.read_csv(tmp_path / "qc_retained_all_null_columns.csv")
    assert qc.iloc[0].evidence_status == "retained_source_blank"
    assert qc.iloc[0].actual_source_rows == 2
    assert qc.iloc[0].source_mismatches == 0


@pytest.mark.parametrize("mutation", [
    "UPDATE release_scalar_raw SET raw_value='9'",
    "UPDATE release_scalar_raw SET value_status='suppressed'",
    "UPDATE release_scalar_raw SET source_csv_sha256='changed'",
    "UPDATE release_scalar_raw SET year=2023",
    "UPDATE release_scalar_raw SET access_table_name='OTHER2024'",
    "DELETE FROM release_scalar_raw",
    "UPDATE release_direct_mapped SET variable_id='other'",
    "UPDATE release_direct_mapped SET normalized_value=4",
    "UPDATE release_canonical_column_coverage SET non_null_value_rows=1",
    "UPDATE release_canonical_column_coverage SET eligible_value_rows=0",
])
def test_retained_null_exception_rejects_changed_evidence(tmp_path, mutation):
    con, contract = fixture(tmp_path)
    con.execute(mutation)
    with pytest.raises(SystemExit, match="source evidence changed"):
        validate_retained_null_columns(con, [2024], tmp_path, contract)


def test_unlisted_null_column_and_different_year_remain_failures(tmp_path):
    con, contract = fixture(tmp_path)
    with pytest.raises(SystemExit, match="exactly year 2024"):
        validate_retained_null_columns(con, [2023], tmp_path, contract)
    con.execute("INSERT INTO release_canonical_column_coverage VALUES ('UNREVIEWED',2,0)")
    with pytest.raises(SystemExit, match="Unreviewed.*UNREVIEWED"):
        validate_retained_null_columns(con, [2024], tmp_path, contract)


def test_checked_in_exception_is_only_the_eleven_reviewed_fields():
    root = Path(__file__).resolve().parents[1]
    approved = pd.read_csv(root / "contracts/extension_2024/retained_all_null_columns.csv")
    expected = {"CHG10AY0", "CHG10AY1", "CHG10AY2", "CHG10PY0", "CHG10PY1", "CHG10PY2",
                "PCADM_F", "PCCOS_F", "PCGR2_F", "PCGR_F", "PCOM_F"}
    assert set(approved.analysis_column) == expected
    assert set(approved.year) == {2024}
    assert approved.source_rows.sum() == 65562
