-------------------------------------------------
--           TOP 5 NATIONS BY REVENUE          --
-------------------------------------------------
SELECT
    c.c_nation,
    ROUND(AVG(l.l_revenue), 2) AS avg_revenue
FROM fact_lineitem AS l
JOIN dim_customer AS c ON c.c_custkey = l.l_custkey
GROUP BY c.c_nation
ORDER BY avg_revenue DESC
LIMIT 5;
-------------------------------------------------



-------------------------------------------------
-- MOST USED SHIPPING MODE AMONG TOP 5 NATIONS --
-------------------------------------------------
WITH top_5_nation AS (
    SELECT
        c.c_nation,
        ROUND(AVG(l.l_revenue), 2) AS avg_revenue
    FROM fact_lineitem AS l
    JOIN dim_customer AS c ON c.c_custkey = l.l_custkey
    GROUP BY c.c_nation
    ORDER BY avg_revenue DESC
    LIMIT 5
)
SELECT
    l.l_shipmode,
    COUNT(1) AS frequency
FROM fact_lineitem AS l
JOIN dim_customer c ON c.c_custkey = l.l_custkey
WHERE c.c_nation IN (SELECT c_nation FROM top_5_nation)
GROUP BY l.l_shipmode
LIMIT 3;
-------------------------------------------------



-------------------------------------------------
--            TOP 5 SELLING MONTHS             --
-------------------------------------------------
SELECT
    d.d_monthname,
    ROUND(AVG(l.l_revenue), 2) AS avg_revenue
FROM fact_lineitem AS l
JOIN dim_date AS d ON d.d_datekey = l.l_orderdatekey
GROUP BY d_monthname
ORDER BY avg_revenue DESC
LIMIT 5;
-------------------------------------------------



-------------------------------------------------
--          TOP 5 CUSTOMERS (REVENUE)          --
-------------------------------------------------
SELECT
    c.c_custkey,
    c.c_name AS customer_name,
    ROUND(AVG(l.l_revenue), 2) AS avg_revenue
FROM fact_lineitem AS l
JOIN dim_customer AS c ON c.c_custkey = l.l_custkey
GROUP BY c.c_custkey
ORDER BY avg_revenue DESC
LIMIT 5;
-------------------------------------------------




-------------------------------------------------
--         TOP 5 CUSTOMERS (QUANTITY)          --
-------------------------------------------------
SELECT
    c.c_custkey,
    c.c_name AS customer_name,
    ROUND(AVG(l.l_quantity), 2) AS avg_quantity
FROM fact_lineitem AS l
JOIN dim_customer AS c ON c.c_custkey = l.l_custkey
GROUP BY c.c_custkey
ORDER BY avg_quantity DESC
LIMIT 5;
-------------------------------------------------



-------------------------------------------------
--     PREVIOUS VS. CURRENT PERIOD REVENUE     --
-------------------------------------------------
SELECT
    current_period.d_yearmonth,
    current_period.avg_revenue AS current_avg_revenue,
    CASE
        WHEN prev_period.d_yearmonth IS NULL
            THEN 'N/A'
        ELSE prev_period.d_yearmonth
    END AS prev_d_yearmonth,
    CASE
        WHEN prev_period.avg_revenue IS NULL
            THEN 0
        ELSE prev_period.avg_revenue
    END AS prev_period_avg_revenue
FROM (
    SELECT
        ROUND(AVG(l_revenue), 2) AS avg_revenue,
        d.d_month,
        d_yearmonth
    FROM fact_lineitem AS l
    JOIN dim_date AS d ON d.d_datekey = l.l_orderdatekey
    WHERE d_year = 1997
    GROUP BY d_month, d_yearmonth
) AS current_period
LEFT JOIN (
    SELECT
        ROUND(AVG(l_revenue), 2) AS avg_revenue,
        d.d_month,
        d.d_yearmonth
    FROM fact_lineitem AS l
    JOIN dim_date AS d ON d.d_datekey = l.l_orderdatekey
    WHERE d_year = 1998
    GROUP BY d_month, d_yearmonth
) AS prev_period ON prev_period.d_month = current_period.d_month
ORDER BY d_yearmonth;