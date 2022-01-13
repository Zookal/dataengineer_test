WITH supplier AS (
  SELECT * FROM {{ source('staging', 'stg_supplier') }}
),
nation AS (
  SELECT * FROM {{ source('staging', 'stg_nation') }}
),
region AS (
  SELECT * FROM {{ source('staging', 'stg_region') }}
)

SELECT
  s.suppkey,
  s.name,
  s.address,
  n.nationkey,
  n.name AS nationname,
  r.regionkey,
  r.name AS regionname,
  s.phone,
  s.acctbal
FROM supplier AS s
  INNER JOIN nation AS n
    ON s.nationkey = n.nationkey
  INNER JOIN region AS r
    ON n.regionkey = r.regionkey
ORDER BY s.suppkey