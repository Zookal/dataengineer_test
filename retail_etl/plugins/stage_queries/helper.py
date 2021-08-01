from typing import Optional

from stage_queries.region import get_upsert_query as _get_region_upsert_query
from stage_queries.customer import get_upsert_query as _get_customer_upsert_query
from stage_queries.order import get_upsert_query as _get_order_upsert_query
from stage_queries.lineitem import get_upsert_query as _get_lineitem_upsert_query
from stage_queries.nation import get_upsert_query as _get_nation_upsert_query
from stage_queries.part import get_upsert_query as _get_part_upsert_query
from stage_queries.supplier import get_upsert_query as _get_supplier_upsert_query
from stage_queries.partsupp import get_upsert_query as _get_partsupp_upsert_query

_UPSERT_QUERIES = {
    "region": _get_region_upsert_query(),
    "customer": _get_customer_upsert_query(),
    "orders": _get_order_upsert_query(),
    "lineitem": _get_lineitem_upsert_query(),
    "nation": _get_nation_upsert_query(),
    "part": _get_part_upsert_query(),
    "supplier": _get_supplier_upsert_query(),
    "partsupp": _get_partsupp_upsert_query(),
}


def get_table_upsert_query(table_name: str) -> Optional[str]:
    """Get the Upsert SQL Query for a given table name."""
    return _UPSERT_QUERIES.get(table_name)
