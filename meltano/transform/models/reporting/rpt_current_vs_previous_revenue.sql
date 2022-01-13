WITH fct_lineitem AS (
  SELECT * FROM {{ source('analytics_star', 'fct_lineitem') }}
), dim_date AS (
  SELECT * FROM {{ source('analytics_star', 'dim_date') }}
)

SELECT
  d.month_of_year,
  d.month_name,
  SUM(l.revenue) AS current_revenue,
  LAG(SUM(l.revenue)) OVER (ORDER BY d.month_of_year) AS previous_revenue,
  ROUND(
    (SUM(l.revenue) - LAG(SUM(l.revenue)) OVER (ORDER BY d.month_of_year))
    / LAG(SUM(l.revenue)) OVER (ORDER BY d.month_of_year) * 100, 2
  ) AS growth_percent
FROM fct_lineitem AS l
  INNER JOIN dim_date AS d
    ON l.orderdate::DATE = d.date_day
WHERE d.year_number = 1994
GROUP BY d.month_of_year, month_name 
ORDER BY d.month_of_year