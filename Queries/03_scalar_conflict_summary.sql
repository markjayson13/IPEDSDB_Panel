SELECT
  year,
  COUNT(*) AS conflict_rows,
  COUNT(DISTINCT UNITID) AS affected_unitids,
  COUNT(DISTINCT COALESCE(source_file, '') || '|' || COALESCE(varnumber, '')) AS affected_source_var_pairs
FROM inspect.scalar_conflicts
GROUP BY 1
ORDER BY 1;
