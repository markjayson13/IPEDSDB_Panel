"""
Tests for NCES Access release-page parsing behavior.
"""
from __future__ import annotations

from helpers import load_script_module


download_mod = load_script_module("download_access_databases", "Scripts/01_download_access_databases.py")


def test_parse_access_release_table_filters_rows() -> None:
    html = """
    <html>
      <body>
        <table>
          <tr>
            <th>Database Name</th><th>Documentation</th><th>Release Type</th><th>Release Date</th>
          </tr>
          <tr>
            <td><a href="/ipeds/tablefiles/zipfiles/IPEDS_2023-24_Final.zip">2023-24 Access</a></td>
            <td><a href="/ipeds/tablefiles/tableDocs/IPEDS202324Tablesdoc.xlsx">2023-24 Excel</a></td>
            <td>Final</td>
            <td>March 2026</td>
          </tr>
          <tr>
            <td><a href="/ipeds/tablefiles/zipfiles/IPEDS_2024-25_Provisional.zip">2024-25 Access</a></td>
            <td><a href="/ipeds/tablefiles/tableDocs/IPEDS202425Tablesdoc.xlsx">2024-25 Excel</a></td>
            <td>Provisional</td>
            <td>March 2026</td>
          </tr>
        </table>
      </body>
    </html>
    """
    rows = download_mod.parse_access_release_table(html)
    assert len(rows) == 2
    assert rows[0]["year"] == 2023
    assert rows[0]["release_type"] == "Final"
    assert rows[0]["access_filename"] == "IPEDS_2023-24_Final.zip"
    assert rows[1]["year"] == 2024
    assert rows[1]["release_type"] == "Provisional"
