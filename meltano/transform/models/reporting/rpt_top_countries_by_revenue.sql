WITH fct_lineitem AS (
  SELECT * FROM {{ ref('fct_lineitem') }}
), dim_customer AS (
  SELECT * FROM {{ ref('dim_customer') }}
)

SELECT
    c.nationname,
    ROUND(AVG(l.revenue), 2) AS avg_revenue
FROM fct_lineitem AS l
INNER JOIN dim_customer AS c
  ON c.custkey = l.custkey
GROUP BY c.nationname
ORDER BY avg_revenue DESC
LIMIT 5