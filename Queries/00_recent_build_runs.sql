SELECT
  started_at,
  years_spec,
  lane_split,
  typed_output,
  persist_duckdb,
  json_extract_string(config_json, '$.duckdb_memory_limit') AS duckdb_memory_limit,
  json_extract_string(config_json, '$.scalar_conflict_buckets') AS scalar_conflict_buckets
FROM inspect.build_runs
ORDER BY started_at DESC
LIMIT 20;
