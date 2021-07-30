from typing import Dict
import traceback

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from mysql.connector.errors import OperationalError

from custom_operators.tbl_to_staging import helper


class TblToStageOperator(BaseOperator):
    def __init__(self, mysql_conn_id: str, table_name: str, pandas_read_args: Dict, *args, **kwargs):
        """
        Operator to import a .tbl file to a MySQL database.

        :param mysql_conn_id: The registered connection ID of MySQL connection.
        :param pandas_read_args: The arguments that will be used for reading the .tbl files
        :param args: Any additional args that BaseOperator can use.
        :param kwargs: Any additional keyword args that BaseOperator can use.
        """
        super().__init__(*args, **kwargs)
        self._mysql_conn_id = mysql_conn_id
        self._pandas_read_args = pandas_read_args
        self._table_name = table_name

    def execute(self, context: Dict):
        self.log.info("TblToStageOperator Starting...")
        mysql_hook = MySqlHook(mysql_conn_id=self._mysql_conn_id)
        mysql_engine = mysql_hook.get_sqlalchemy_engine()

        df_batches = helper.get_dataframe(**self._pandas_read_args, table_name=self._table_name)
        total_inserted_rows = 0

        for _df in df_batches:  # type: pd.DataFrame
            self.log.debug(f"Inserting {len(_df)} rows in the {self._table_name} table...")
            try:
                # TODO: if_exists=append
                _df.to_sql(name=self._table_name, index=False, con=mysql_engine, if_exists="replace")
                total_inserted_rows += 1
            except Exception as _:
                self.log.debug(f"The TblToStageOperator process has failed {traceback.format_exc()}")
