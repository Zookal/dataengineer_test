from typing import Optional

from dw_queries.dim_part import (
    get_upsert_query as _get_dim_part_upsert_query,
    get_select_query_for_insert as _get_dim_part_select_query_for_insert,
)
from dw_queries.dim_supplier import (
    get_upsert_query as _get_dim_supplier_upsert_query,
    get_select_query_for_insert as _get_dim_supplier_select_query_for_insert,
)
from dw_queries.dim_customer import (
    get_upsert_query as _get_dim_customer_upsert_query,
    get_select_query_for_insert as _get_dim_customer_select_query_for_insert,
)
from dw_queries.dim_date import (
    get_upsert_query as _get_dim_date_upsert_query,
    get_select_query_for_insert as _get_dim_date_select_query_for_insert,
)
from dw_queries.fact_lineitem import (
    get_upsert_query as _get_fact_lineitem_upsert_query,
    get_select_query_for_insert as _get_fact_lineitem_select_query_for_insert,
)


_DIM_UPSERT_QUERIES = {
    "dim_part": _get_dim_part_upsert_query(),
    "dim_supplier": _get_dim_supplier_upsert_query(),
    "dim_customer": _get_dim_customer_upsert_query(),
    "dim_date": _get_dim_date_upsert_query(),
    "fact_lineitem": _get_fact_lineitem_upsert_query(),
}


_DIM_SELECT_QUERIES = {
    "dim_part": _get_dim_part_select_query_for_insert(),
    "dim_supplier": _get_dim_supplier_select_query_for_insert(),
    "dim_customer": _get_dim_customer_select_query_for_insert(),
    "dim_date": _get_dim_date_select_query_for_insert(),
    "fact_lineitem": _get_fact_lineitem_select_query_for_insert(),
}


def get_dw_table_upsert_query(table_name: str) -> Optional[str]:
    """Get the Upsert SQL Query for a given table name for the Data Warehouse."""
    return _DIM_UPSERT_QUERIES.get(table_name)


def get_dw_table_select_query(table_name: str) -> Optional[str]:
    """Get the Select Query to be used on inserting data to the Data Warehouse."""
    return _DIM_SELECT_QUERIES.get(table_name)
