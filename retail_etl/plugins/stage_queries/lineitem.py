def get_upsert_query():
    return """
    INSERT INTO lineitem(
        l_orderkey,
        l_partkey,
        L_suppkey,
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        l_linenumber = VALUES(l_linenumber),
        l_quantity = VALUES(l_quantity),
        l_extendedprice = VALUES(l_extendedprice),
        l_discount = VALUES(l_discount),
        l_tax = VALUES(l_tax),
        l_returnflag = VALUES(l_returnflag),
        l_linestatus = VALUES(l_linestatus),
        l_shipdate = VALUES(l_shipdate),
        l_commitdate = VALUES(l_commitdate),
        l_receiptdate = VALUES(l_receiptdate),
        l_shipinstruct = VALUES(l_shipinstruct),
        l_shipmode = VALUES(l_shipmode),
        l_comment = VALUES(l_comment)
    """
