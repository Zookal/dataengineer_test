def get_upsert_query():
    return """
    INSERT INTO partsupp(
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
    )
    VALUES (
        %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        ps_availqty = VALUES(ps_availqty),
        ps_supplycost = VALUES(ps_supplycost),
        ps_comment = VALUES(ps_comment)
    """
