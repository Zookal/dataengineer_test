from typing import Dict

from airflow.models import BaseOperator

from custom_operators.postgres_dw_operator import helper


class PostgresDwOperator(BaseOperator):
    def __init__(self, pandas_read_config: Dict, postgres_load_config: Dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pandas_read_config = pandas_read_config
        self._postgres_load_config = postgres_load_config

    def execute(self, context: Dict):
        self.log.info("PostgresDwOperator Starting...")
        execution_ts = context.get("execution_date")
        df_batches = helper.get_dataframe(**self._pandas_read_config, execution_ts=execution_ts)
        total_inserted_rows = helper.load_to_postgres_dw(
            **self._postgres_load_config, df_batches=df_batches, execution_ts=execution_ts
        )
        table_name = self._postgres_load_config.get("table_name")

        self.log.info(f"Finished Loading {total_inserted_rows} rows in the {table_name} table.")
