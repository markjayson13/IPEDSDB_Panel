SELECT
  year,
  COUNT(*) AS unitid_year_rows,
  COUNT(DISTINCT UNITID) AS distinct_unitids
FROM inspect.panel_clean
GROUP BY 1
ORDER BY 1;
