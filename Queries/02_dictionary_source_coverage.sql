SELECT
  year,
  source_file,
  COUNT(*) AS variable_rows,
  COUNT(*) FILTER (
    WHERE longDescription IS NOT NULL
      AND TRIM(longDescription) <> ''
  ) AS with_long_description
FROM inspect.dictionary_lake
GROUP BY 1, 2
ORDER BY 1, 2;
