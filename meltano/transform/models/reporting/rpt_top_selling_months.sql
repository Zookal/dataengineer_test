WITH fct_lineitem AS (
  SELECT * FROM {{ ref('fct_lineitem') }}
), dim_date AS (
  SELECT * FROM {{ ref('dim_date') }}
)

SELECT
  d.month_name,
  SUM(l.revenue)
FROM fct_lineitem AS l
  INNER JOIN dim_date AS d
    ON l.orderdate::DATE = d.date_day
GROUP BY d.month_name
ORDER BY SUM DESC