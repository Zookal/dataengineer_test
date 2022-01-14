WITH lineitem AS (
  SELECT * FROM {{ source('staging', 'stg_lineitem') }}
), orders AS (
  SELECT * FROM {{ source('staging', 'stg_orders') }}
), customer AS (
  SELECT * FROM {{ source('staging', 'stg_customer') }}
)

SELECT
  l.orderkey,
  l.partkey,
  l.suppkey,
  c.custkey,
  l.linenumber,
  l.quantity,
  l.extendedprice,
  l.discount,
  l.extendedprice - (l.extendedprice * l.discount) AS revenue,
  l.tax,
  l.returnflag,
  l.linestatus,
  o.orderdate::DATE,
  l.shipdate::DATE,
  l.commitdate::DATE,
  l.receiptdate::DATE,
  l.shipinstruct,
  l.shipmode
FROM lineitem AS l
  INNER JOIN orders AS o
    ON l.orderkey = o.orderkey
  INNER JOIN customer AS c
    ON o.custkey = c.custkey