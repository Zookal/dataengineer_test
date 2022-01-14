WITH dates AS (
  {{ dbt_date.get_date_dimension('1992-01-01', '1998-12-31') }}
)

SELECT
  *
FROM dates