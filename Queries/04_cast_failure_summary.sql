SELECT
  year,
  column,
  non_empty_tokens,
  parsed_numeric_tokens,
  failed_parse_tokens
FROM inspect.cast_report
WHERE failed_parse_tokens > 0
ORDER BY failed_parse_tokens DESC, year, column
LIMIT 100;
