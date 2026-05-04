# Contributing

IPEDSDB_Panel is maintained by Mark Jayson Farol as sole researcher and code owner.

Current contact information is available at `https://markjayson.com`.

Issues are welcome when they include enough evidence to reproduce the concern. Pull requests may be reviewed when they are small, scoped, and tied to an issue or release concern.

Good issue reports include:

- the release version or git commit
- the affected years, tables, or variables
- the command that failed or the file that looks wrong
- a short expected-versus-observed statement
- paths to QA evidence when available

Before opening a pull request, run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q
python3 Scripts/QA_QC/11_validate_panel_contract.py
python3 Scripts/QA_QC/18_public_release_guard.py
python3 Scripts/QA_QC/19_docs_style_guard.py
python3 Scripts/QA_QC/05_repo_size_guard.py --max-file-size-mb 5 --top-n 25
```

Do not commit generated data, parquet files, DuckDB files, Access downloads, or local build folders. The generated data root belongs outside git under `IPEDSDB_ROOT`.
