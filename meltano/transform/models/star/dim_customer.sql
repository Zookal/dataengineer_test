WITH customer AS (
  SELECT * FROM {{ source('staging', 'stg_customer') }}
),
nation AS (
  SELECT * FROM {{ source('staging', 'stg_nation') }}
),
region AS (
  SELECT * FROM {{ source('staging', 'stg_region') }}
),
ranking AS (
  SELECT
    c.custkey,
    c.acctbal,
    NTILE(5) OVER (ORDER BY c.acctbal ASC) AS quintile
  FROM customer AS c
)

SELECT
  c.custkey,
  c.name,
  c.address,
  n.nationkey,
  n.name AS nationname,
  r.regionkey,
  r.name AS regionname,
  c.phone,
  c.acctbal,
  CASE
    WHEN rk.quintile = 5 THEN 'Gold'
    WHEN rk.quintile = 4 THEN 'Silver'
    ELSE 'Bronze'
  END AS tier,
  c.mktsegment
FROM customer AS c
  INNER JOIN nation AS n
    ON c.nationkey = n.nationkey
  INNER JOIN region AS r
    ON n.regionkey = r.regionkey
  INNER JOIN ranking as rk
    ON c.custkey = rk.custkey
ORDER BY c.custkey