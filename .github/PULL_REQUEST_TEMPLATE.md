# Pull request

## Scope

Describe the change and the release concern it addresses.

## Checks run

- [ ] `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q`
- [ ] `python3 Scripts/QA_QC/11_validate_panel_contract.py`
- [ ] `python3 Scripts/QA_QC/18_public_release_guard.py`
- [ ] `python3 Scripts/QA_QC/19_docs_style_guard.py`
- [ ] `python3 Scripts/QA_QC/05_repo_size_guard.py --max-file-size-mb 5 --top-n 25`

## Data files

- [ ] This pull request does not commit generated data, parquet files, DuckDB files, Access downloads, or local build folders.
