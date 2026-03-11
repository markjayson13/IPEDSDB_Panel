SELECT
  year,
  academic_year_label,
  release_type,
  release_date_text,
  download_status
FROM inspect.release_inventory
ORDER BY year;
