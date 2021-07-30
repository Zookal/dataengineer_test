from typing import Dict, Union

import pandas as pd
from pandas.io.parsers import TextFileReader

from custom_operators.tbl_to_staging import model


_HEADER_MAPPING = {
    "region": model.RegionHeader(),
    "nation": model.NationHeader(),
    "part": model.PartHeader(),
    "customer": model.CustomerHeader(),
    "supplier": model.SupplierHeader(),
    "order": model.OrderHeader(),
    "partsupp": model.PartSuppHeader(),
    "lineitem": model.LineItemHeader(),
}


def _get_header(
    *, table_name: str
) -> Union[
    model.RegionHeader,
    model.NationHeader,
    model.PartHeader,
    model.CustomerHeader,
    model.SupplierHeader,
    model.OrderHeader,
    model.PartSuppHeader,
    model.LineItemHeader,
    None,
]:
    return _HEADER_MAPPING.get(table_name)


def get_dataframe(table_name: str, **pandas_read_args: Dict) -> TextFileReader:
    """Get the corresponding DataFrame of a given file."""
    header = _get_header(table_name=table_name)
    columns = header.to_list() if header else None
    return pd.read_csv(**pandas_read_args, names=columns, usecols=columns)
