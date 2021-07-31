from typing import Dict
import traceback

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from custom_operators.tbl_to_staging import helper


class TblToStageOperator(BaseOperator):
    def __init__(
        self, mysql_conn_id: str, table_name: str, pandas_read_args: Dict, upsert_query: str, *args, **kwargs
    ):
        """
        Operator to import a .tbl file to a MySQL database.

        :param mysql_conn_id: The registered connection ID of MySQL connection.
        :param pandas_read_args: The arguments that will be used for reading the .tbl files
        :param upsert_query: The upsert query for the corresponding given table.
        :param args: Any additional args that BaseOperator can use.
        :param kwargs: Any additional keyword args that BaseOperator can use.
        """
        super().__init__(*args, **kwargs)
        self._mysql_conn_id = mysql_conn_id
        self._pandas_read_args = pandas_read_args
        self._table_name = table_name
        self._upsert_query = upsert_query

    def execute(self, context: Dict):
        self.log.info("TblToStageOperator Starting...")
        mysql_hook = MySqlHook(mysql_conn_id=self._mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()

        df_batches = helper.get_dataframe(**self._pandas_read_args, table_name=self._table_name)
        total_inserted_rows = 0

        for _df in df_batches:  # type: pd.DataFrame
            nrows = len(_df)
            self.log.info(f"Inserting {nrows} rows in the {self._table_name} table...")

            try:
                rows = (tuple(row) for row in _df.itertuples(index=False))
                mysql_cursor.executemany(self._upsert_query, rows)
                total_inserted_rows += nrows
            except Exception:
                mysql_conn.rollback()
                self.log.info(f"The TblToStageOperator process has failed {traceback.format_exc()}")
            finally:
                mysql_conn.commit()

        mysql_conn.close()
        self.log.info(f"Finished Inserting {total_inserted_rows} rows in the {self._table_name} table.")
