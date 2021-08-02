def get_upsert_query():
    return """
    INSERT INTO dim_part(
        p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (p_partkey)
    DO UPDATE SET
        (p_name, p_mfgr, p_brand, p_type, p_size, p_container)
        = (
            EXCLUDED.p_name,
            EXCLUDED.p_mfgr,
            EXCLUDED.p_brand,
            EXCLUDED.p_type,
            EXCLUDED.p_size,
            EXCLUDED.p_container
        );
    """


def get_select_query_for_insert():
    return """
    SELECT DISTINCT
        p.p_partkey,
        p.p_name,
        p.p_mfgr,
        p.p_brand,
        p.p_type,
        p.p_size,
        p.p_container
    FROM part AS p
    WHERE p.updated_at
        BETWEEN '{yesterday}'
            AND '{today}'
    """
