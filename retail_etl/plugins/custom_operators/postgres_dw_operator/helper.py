import traceback
from datetime import timedelta
from typing import Dict, Optional, Iterator
import logging

import pandas as pd
import pendulum
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_logger(logger_name: Optional[str]) -> logging.Logger:
    logger = logging.getLogger(logger_name if logger_name else "airflow.task")
    return logger


def _create_mysql_connection(mysql_conn_id: str):
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    mysql_conn = mysql_hook.get_conn()
    return mysql_conn


def _create_postgres_connection(postgres_conn_id: str):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()
    return postgres_conn


def get_dataframe(execution_ts: pendulum.DateTime, **pandas_read_args: Dict) -> Iterator[pd.DataFrame]:
    """Performs Select query to get a DataFrame from MySQL DB."""
    connection = _create_mysql_connection(mysql_conn_id=pandas_read_args.pop("mysql_conn_id"))
    query = pandas_read_args.pop("sql").format(
        today=execution_ts + timedelta(minutes=15), yesterday=execution_ts - timedelta(days=1)
    )
    df_batches = pd.read_sql(**pandas_read_args, sql=query, con=connection)
    return df_batches


def load_to_postgres_dw(
    df_batches: Iterator[pd.DataFrame], execution_ts: pendulum.DateTime, **data_load_args: Dict
) -> int:
    """
    Loads batches of data to Postgres Data Warehouse.

    :param df_batches: An iterator like object
        which contains the batches of DataFrames to be loaded.
    :param data_load_args: The arguments needed to connect
        and perform insert query into the Database.
    :param execution_ts: The timestamp when this function was called.

    :return: Total number of rows successfully loaded.
    """
    logger = _get_logger(logger_name=data_load_args.get("logger_name"))
    postgres_conn = _create_postgres_connection(
        postgres_conn_id=data_load_args.get("postgres_default", "postgres_default")
    )
    postgres_cursor = postgres_conn.cursor()
    table_name = data_load_args.get("table_name")
    total_inserted_rows = 0
    for idx, _df in enumerate(df_batches):  # type: int, pd.DataFrame
        nrows = len(_df)
        logger.info(f"Inserting {nrows} rows in the {table_name} table...")
        try:
            rows = (tuple(row) for row in _df.itertuples(index=False))
            upsert_query = data_load_args.get("upsert_query", "")
            postgres_cursor.executemany(upsert_query, rows)
            postgres_conn.commit()
        except Exception:
            postgres_conn.rollback()
            logger.info("The PostgresDwOperator process has failed", exc_info=traceback.format_exc())
        finally:
            postgres_cursor.execute(
                f"SELECT COUNT(CASE WHEN updated_at >= '{execution_ts}' THEN 1 ELSE NULL END) "
                f"FROM {table_name};"
            )
            total_inserted_rows = postgres_cursor.fetchone()[0]
    postgres_conn.close()
    return total_inserted_rows
