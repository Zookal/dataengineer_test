def get_upsert_query():
    return """
    INSERT INTO `orders`(
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        o_orderkey = VALUES(o_orderkey),
        o_custkey = VALUES(o_custkey),
        o_orderstatus = VALUES(o_orderstatus),
        o_totalprice = VALUES(o_totalprice),
        o_orderdate = VALUES(o_orderdate),
        o_orderpriority = VALUES(o_orderpriority),
        o_clerk = VALUES(o_clerk),
        o_shippriority = VALUES(o_shippriority),
        o_comment = VALUES(o_comment)
    """
