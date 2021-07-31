from typing import Dict

from airflow.models import BaseOperator

from custom_operators.tbl_to_staging import helper


class TblToStageOperator(BaseOperator):
    def __init__(self, pandas_read_args: Dict, data_load_args: Dict, *args, **kwargs):
        """
        Operator to import a .tbl file to a MySQL database.

        :param pandas_read_args: The arguments that will be used for reading the .tbl files
        :param data_load_args: A key/value pair with the args that will be used for loading a batch of data.
        :param args: Any additional args that BaseOperator can use.
        :param kwargs: Any additional keyword args that BaseOperator can use.
        """
        super().__init__(*args, **kwargs)
        self._data_load_args = data_load_args
        self._pandas_read_args = pandas_read_args

    def execute(self, context: Dict):
        self.log.info("TblToStageOperator Starting...")

        df_batches = helper.get_dataframe(**self._pandas_read_args)
        total_inserted_rows = helper.load_to_mysql_db(**self._data_load_args, df_batches=df_batches)
        table_name = self._data_load_args.get("table_name")

        self.log.info(f"Finished Loading {total_inserted_rows} rows in the {table_name} table.")
