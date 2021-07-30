def get_upsert_query():
    return """
    INSERT INTO region(
        r_regionkey, r_name, r_comment
    )
    VALUES (
        %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE 
        r_regionkey = VALUES(r_regionkey), r_name = VALUES(r_name), r_comment = VALUES(r_comment)
    """