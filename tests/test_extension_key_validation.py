from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from panel_extension import validate_keys


@pytest.mark.parametrize("unitid, year", [
    (100654, 2024.5),
    (100654.5, 2024),
    (float("inf"), 2024),
    (float("nan"), 2024),
    (None, 2024),
    (100654, None),
    (100654, float("nan")),
    (100654, float("inf")),
    (0, 2024),
    (-2, 2024),
])
def test_fractional_or_nonfinite_identifiers_are_not_valid_panel_keys(tmp_path, unitid, year):
    path = tmp_path / "invalid.parquet"
    pq.write_table(pa.table({"UNITID": [unitid], "year": [year]}), path)
    with pytest.raises(ValueError):
        validate_keys(path, [2024])
