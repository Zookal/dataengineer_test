def get_upsert_query():
    return """
    INSERT INTO part(
        p_partkey,
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size,
        p_container,
        p_retailprice,
        p_comment
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        p_partkey = VALUES(p_partkey),
        p_name = VALUES(p_name),
        p_mfgr = VALUES(p_mfgr),
        p_brand = VALUES(p_brand),
        p_type = VALUES(p_type),
        p_size = VALUES(p_size),
        p_container = VALUES(p_container),
        p_retailprice = VALUES(p_retailprice),
        p_comment = VALUES(p_comment)
    """
