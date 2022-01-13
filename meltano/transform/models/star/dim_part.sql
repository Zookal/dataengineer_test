WITH part AS (
  SELECT * FROM {{ source('staging', 'stg_part') }}
)

SELECT
  p.partkey,
  p.name,
  p.mfgr,
  p.brand,
  p.type,
  p.size,
  p.container,
  p.retailprice
FROM part AS p
ORDER BY p.partkey