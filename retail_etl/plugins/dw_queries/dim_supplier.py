def get_upsert_query():
    return """
    INSERT INTO dim_supplier(
        s_suppkey, s_name, s_address, s_nation, s_phone
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (s_suppkey)
    DO UPDATE SET
        (s_name, s_address, s_nation, s_phone)
        = (
            EXCLUDED.s_name,
            EXCLUDED.s_address,
            EXCLUDED.s_nation,
            EXCLUDED.s_phone
        );
    """


def get_select_query_for_insert():
    return """
    SELECT DISTINCT
        s.s_suppkey,
        s.s_name,
        s.s_address,
        n.n_name AS s_nation,
        s.s_phone 
    FROM supplier AS s
    JOIN nation AS n ON n.n_nationkey = s.s_nationkey
    WHERE s.updated_at BETWEEN '{yesterday}' AND '{today}'
    """