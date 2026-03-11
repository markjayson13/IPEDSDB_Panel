# Saved Queries

This folder holds starter SQL for inspecting the persisted DuckDB build and the standard panel outputs.

Use the query runner:

```bash
python Scripts/run_saved_query.py --list
python Scripts/run_saved_query.py 01_clean_panel_rows_by_year
```

What the runner does:

- opens an in-memory DuckDB inspection session
- attaches the persisted build database when it exists
- creates stable `inspect.*` views over the main panel and dictionary artifacts
- writes a timestamped result folder under `IPEDSDB_ROOT/Checks/query_results/`

Most useful inspection views:

- `inspect.panel_long`
- `inspect.panel_wide`
- `inspect.panel_clean`
- `inspect.dictionary_lake`
- `inspect.dictionary_codes`
- `inspect.release_inventory`
- `inspect.build_runs`
- `inspect.scalar_conflicts`
- `inspect.cast_report`
- `inspect.wide_year_summary`

The SQL here is intentionally conservative: it should stay readable and run against the standard local output layout without manual path editing.
