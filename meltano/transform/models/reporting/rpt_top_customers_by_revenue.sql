WITH fct_lineitem AS (
  SELECT * FROM {{ source('analytics_star', 'fct_lineitem') }}
), dim_customer AS (
  SELECT * FROM {{ source('analytics_star', 'dim_customer') }}
)

SELECT
  c.name,
  SUM(l.revenue)
FROM fct_lineitem AS l
  INNER JOIN dim_customer AS c
    ON l.custkey = c.custkey
GROUP BY c.name
ORDER BY SUM DESC
LIMIT 10