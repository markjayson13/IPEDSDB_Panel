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

## Get involved

Ask questions in [Q&A discussions](https://github.com/markjayson13/IPEDSDB_Panel/discussions/categories/q-a). Share a paper, class, or replication in [Show and tell](https://github.com/markjayson13/IPEDSDB_Panel/discussions/categories/show-and-tell).

The issue forms apply the repository labels:

- [Bug report](.github/ISSUE_TEMPLATE/bug_report.yml) applies `bug`. Choosing Roadmap also applies `roadmap`.
- [Data quality report](.github/ISSUE_TEMPLATE/data_quality_report.yml) applies `data quality`. Choosing New data year also applies `new data year`.
- [Variable request](.github/ISSUE_TEMPLATE/variable_request.yml) applies `variable request`. Choosing New data year also applies `new data year`.
- Show and tell applies `research use case`.

The public dictionary is the codebook at https://markjayson13.github.io/IPEDSDB_Panel/ and `docs/codebook/codebook.csv`. A mapping conflict still has a [variable mapping](.github/ISSUE_TEMPLATE/variable_mapping.yml) form, a failed rebuild has a [reproducibility failure](.github/ISSUE_TEMPLATE/reproducibility_failure.yml) form, and a docs correction has a [documentation](.github/ISSUE_TEMPLATE/documentation.yml) form. The `good first issue` label marks a bounded first change.
