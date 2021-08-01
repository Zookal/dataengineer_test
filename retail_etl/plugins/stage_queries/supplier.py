def get_upsert_query():
    return """
    INSERT INTO supplier(
        s_suppkey,
        s_name,
        s_address,
        s_nationkey,
        s_phone,
        s_acctbal,
        s_comment
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        s_suppkey = VALUES(s_suppkey),
        s_name = VALUES(s_name),
        s_address = VALUES(s_address),
        s_nationkey = VALUES(s_nationkey),
        s_phone = VALUES(s_phone),
        s_acctbal = VALUES(s_acctbal),
        s_comment = VALUES(s_comment)
    """
