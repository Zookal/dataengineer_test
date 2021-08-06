def get_upsert_query():
    return """
    INSERT INTO dim_supplier(
        s_id, s_name, s_address, s_nation, s_region, s_phone
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (s_id)
    DO UPDATE SET
        (s_name, s_address, s_nation, s_region, s_phone)
        = (
            EXCLUDED.s_name,
            EXCLUDED.s_address,
            EXCLUDED.s_nation,
            EXCLUDED.s_region,
            EXCLUDED.s_phone
        );
    """


def get_select_query_for_insert():
    return """
    SELECT DISTINCT
        s.s_suppkey AS s_id,
        s.s_name,
        s.s_address,
        n.n_name AS s_nation,
        r.r_name AS s_region,
        s.s_phone
    FROM supplier AS s
    JOIN nation AS n ON n.n_nationkey = s.s_nationkey
    JOIN region AS r ON r.r_regionkey = n.n_regionkey
    WHERE s.updated_at BETWEEN '{yesterday}' AND '{today}'
    """
