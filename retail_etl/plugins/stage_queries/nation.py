def get_upsert_query():
    return """
    INSERT INTO nation(
        n_nationkey,
        n_name,
        n_regionkey,
        n_comment
    )
    VALUES (
        %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        n_nationkey = VALUES(n_nationkey),
        n_name = VALUES(n_name),
        n_regionkey = VALUES(n_regionkey),
        n_comment = VALUES(n_comment)
    """
