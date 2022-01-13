WITH fct_lineitem AS (
  SELECT * FROM {{ source('star', 'fct_lineitem') }}
), dim_customer AS (
  SELECT * FROM {{ source('star', 'dim_customer') }}
), top_5_countries_by_revenue AS (
  SELECT
    c.nationname,
    ROUND(AVG(l.revenue), 2) AS avg_revenue
  FROM fct_lineitem AS l
  INNER JOIN dim_customer AS c ON c.custkey = l.custkey
  GROUP BY c.nationname
  ORDER BY avg_revenue DESC
  LIMIT 5
)

SELECT
  c.nationname,
  MODE() WITHIN GROUP (ORDER BY l.shipmode)
FROM fct_lineitem AS l
  INNER JOIN dim_customer AS c
    ON l.custkey = c.custkey
WHERE c.nationname IN (
  SELECT
    t5.nationname
  FROM top_5_countries_by_revenue AS t5
)
GROUP BY c.nationname