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
    CASE
        WHEN current_period.d_yearmonth IS NULL
            THEN 'N/A'
        ELSE current_period.d_yearmonth
    END AS current_period_d_yearmonth,
    CASE
        WHEN current_period.avg_revenue IS NULL
            THEN 0
        ELSE current_period.avg_revenue
    END AS current_period_avg_revenue,
    prev_period.d_yearmonth AS prev_period_d_yearmonth,
    prev_period.avg_revenue AS prev_period_avg_revenue
FROM (
    SELECT
        ROUND(AVG(l_revenue), 2) AS avg_revenue,
        d.d_month,
        d_yearmonth
    FROM fact_lineitem AS l
    JOIN dim_date AS d ON d.d_datekey = l.l_receiptdatekey
    WHERE d_year = 1997
    GROUP BY d_month, d_yearmonth
) AS prev_period
LEFT JOIN (
    SELECT
        ROUND(AVG(l_revenue), 2) AS avg_revenue,
        d.d_month,
        d.d_yearmonth
    FROM fact_lineitem AS l
    JOIN dim_date AS d ON d.d_datekey = l.l_receiptdatekey
    WHERE d_year = 1998
    GROUP BY d_month, d_yearmonth
) AS current_period ON current_period.d_month = prev_period.d_month
ORDER BY current_period_d_yearmonth, prev_period_d_yearmonth;