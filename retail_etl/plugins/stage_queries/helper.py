from retail_etl.plugins.stage_queries.region import get_upsert_query


_UPSERT_QUERIES = {"region": get_upsert_query()}


def get_table_upsert_query(table_name: str) -> str:
    """Get the Upsert SQL Query for a given table name."""
    return _UPSERT_QUERIES.get(table_name)
