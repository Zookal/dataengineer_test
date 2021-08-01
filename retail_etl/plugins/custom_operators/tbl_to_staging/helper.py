import traceback
from datetime import timedelta
from typing import Dict, Union, Optional
import logging

import pandas as pd
import pendulum
from airflow.models import Connection
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pandas.io.parsers import TextFileReader

from custom_operators.tbl_to_staging import model


_HEADER_MAPPING = {
    "region": model.RegionHeader(),
    "nation": model.NationHeader(),
    "part": model.PartHeader(),
    "customer": model.CustomerHeader(),
    "supplier": model.SupplierHeader(),
    "orders": model.OrderHeader(),
    "partsupp": model.PartSuppHeader(),
    "lineitem": model.LineItemHeader(),
}


def _get_logger(logger_name: Optional[str]) -> logging.Logger:
    logger = logging.getLogger(logger_name if logger_name else "airflow.task")
    return logger


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
    """Map a table name into it's header counterpart."""
    return _HEADER_MAPPING.get(table_name)


def _create_mysql_connection(mysql_conn_id: Optional[str]) -> Connection:
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id if mysql_conn_id else "mysql_default")
    mysql_conn = mysql_hook.get_conn()
    return mysql_conn


def get_dataframe(table_name: str, **pandas_read_args: Dict) -> TextFileReader:
    """Get the corresponding DataFrame of a given file."""
    header = _get_header(table_name=table_name)
    columns = header.to_list() if header else None
    return pd.read_csv(**pandas_read_args, names=columns, usecols=columns)


def load_to_mysql_db(df_batches: TextFileReader, execution_ts: pendulum.DateTime, **data_load_args: Dict) -> int:
    """
    Loads batches of data to MySQL Database.

    :param df_batches: An iterator like object
        which contains the batches of DataFrames to be loaded.
    :param data_load_args: The arguments needed to connect
        and perform insert query into the Database.
    :param: execution_ts: The timestamp when this function was called.

    :return: Total number of rows successfully loaded.
    """
    logger = _get_logger(logger_name=data_load_args.get("logger_name"))
    mysql_conn = _create_mysql_connection(mysql_conn_id=data_load_args.get("mysql_conn_id"))
    mysql_cursor = mysql_conn.cursor()
    table_name = data_load_args.get('table_name')
    total_inserted_rows = 0
    for idx, _df in enumerate(df_batches):  # type: int, pd.DataFrame
        nrows = len(_df)
        logger.debug(f"Inserting {nrows} rows in the {table_name} table...")
        try:
            rows = (tuple(row) for row in _df.itertuples(index=False))
            mysql_cursor.executemany(data_load_args.get("upsert_query"), rows)
        except Exception:
            mysql_conn.rollback()
            logger.error("The TblToStageOperator process has failed", exc_info=traceback.format_exc())
        finally:
            mysql_conn.commit()
            mysql_cursor.execute(
                f"SELECT COUNT(IF(updated_at >= '{execution_ts}', 1, NULL)) FROM {table_name};"
            )
            total_inserted_rows = mysql_cursor.fetchone()[0]
    mysql_conn.close()
    return total_inserted_rows
