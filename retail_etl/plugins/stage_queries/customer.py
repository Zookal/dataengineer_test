def get_upsert_query():
    return """
    INSERT INTO customer(
        c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        c_custkey = VALUES(c_custkey),
        c_name = VALUES(c_name),
        c_address = VALUES(c_address),
        c_phone = VALUES(c_phone),
        c_acctbal = VALUES(c_acctbal),
        c_mktsegment = VALUES(c_mktsegment),
        c_comment = VALUES(c_comment)
    """
