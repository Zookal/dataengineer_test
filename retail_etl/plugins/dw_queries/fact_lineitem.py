def get_upsert_query():
    return """
    INSERT INTO fact_lineitem(
        l_linenumber,
        l_orderkey,
        l_partkey,
        l_suppkey,
        l_custkey,
        l_orderdatekey,
        l_commitdatekey,
        l_receiptdatekey,
        l_shipmode,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_revenue,
        l_tax
    )
    VALUES (
        %s,
        %s,
        (SELECT p_partkey from dim_part WHERE p_id=%s),
        (SELECT s_suppkey from dim_supplier WHERE s_id=%s),
        (SELECT c_custkey from dim_customer WHERE c_id=%s),
        (SELECT d_datekey from dim_date WHERE d_id=%s),
        (SELECT d_datekey from dim_date WHERE d_id=%s),
        (SELECT d_datekey from dim_date WHERE d_id=%s),
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (
        l_linenumber, l_orderkey
    )
    DO UPDATE SET
        (
            l_shipmode,
            l_quantity,
            l_extendedprice,
            l_discount,
            l_revenue,
            l_tax
        )
        = (
            EXCLUDED.l_shipmode,
            EXCLUDED.l_quantity,
            EXCLUDED.l_extendedprice,
            EXCLUDED.l_discount,
            EXCLUDED.l_revenue,
            EXCLUDED.l_tax
        );
    """


def get_select_query_for_insert():
    return """
    SELECT
         l.l_linenumber,
         l.l_orderkey,
         l.l_partkey,
         l.l_suppkey,
         c.c_custkey AS l_custkey,
         DATE_FORMAT(o.o_orderdate, '%Y%m%d') AS l_orderdatekey,
         DATE_FORMAT(l.l_commitdate, '%Y%m%d') AS l_commitdatekey,
         DATE_FORMAT(l.l_receiptdate, '%Y%m%d') AS l_receiptdatekey,
         l.l_shipmode,
         l.l_quantity,
         l.l_extendedprice,
         l.l_discount,
         (l.l_quantity * l.l_extendedprice) - (l.l_extendedprice * l.l_discount) AS l_revenue,
         l.l_tax
    FROM orders AS o
    JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey
    JOIN customer AS c ON c.c_custkey = o.o_custkey
    WHERE l.updated_at BETWEEN '{yesterday}' AND '{today}'
    """
