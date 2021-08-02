def get_upsert_query():
    return """
    INSERT INTO dim_customer(
        c_custkey, c_name, c_address, c_nation, c_region, c_phone, c_mktsegment, c_cluster
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (c_custkey)
    DO UPDATE SET
        (c_name, c_address, c_nation, c_region, c_phone, c_mktsegment, c_cluster)
        = (
            EXCLUDED.c_name,
            EXCLUDED.c_address,
            EXCLUDED.c_nation,
            EXCLUDED.c_region,
            EXCLUDED.c_phone,
            EXCLUDED.c_mktsegment,
            EXCLUDED.c_cluster
        );
    """


def get_select_query_for_insert():
    return """
    SELECT DISTINCT
        c.c_custkey,
        c.c_name,
        c.c_address,
        n.n_name AS c_nation,
        r.r_name AS c_region,
        c.c_phone,
        c.c_mktsegment,
        CASE
            WHEN ranking BETWEEN 0 AND 0.5
                THEN "Bronze Customer"
            WHEN ranking BETWEEN 0.50 AND 0.75
                THEN "Silver Customer"
            WHEN ranking BETWEEN 0.75 AND 1
                THEN "Gold Customer"
        END AS c_cluster
    FROM customer AS c
    JOIN nation AS n ON n.n_nationkey = c.c_nationkey
    JOIN region AS r ON r.r_regionkey = n.n_regionkey
    JOIN (
        SELECT 
            c_custkey,
            c_acctbal,
            PERCENT_RANK() OVER(ORDER BY c_acctbal) AS ranking 
        FROM customer ORDER BY c_acctbal
    ) AS ranking ON ranking.c_custkey = c.c_custkey
    WHERE c.updated_at BETWEEN '{yesterday}' AND '{today}' 
    """