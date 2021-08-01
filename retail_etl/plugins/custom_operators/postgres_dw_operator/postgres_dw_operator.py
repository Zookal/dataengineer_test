from datetime import timedelta
from typing import Dict

from MySQLdb import Connection
import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresDwOperator(BaseOperator):
    def __init__(self, mysql_read_config: Dict, postgres_load_config: Dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mysql_read_config = mysql_read_config
        self._postgres_load_config = postgres_load_config

    def execute(self, context: Dict):
        self.log.info("DataSyncOperator Starting...")
        mysql_conn = self.create_mysql_conn(mysql_conn_id=self._mysql_read_config.get("mysql_conn_id"))
        execution_date = context.get('execution_date')
        part_df = pd.read_sql(
            sql="SELECT p_partkey, p_container, p_name, p_mfgr, p_brand, p_type, p_size "
                "FROM part WHERE updated_at "
                f"BETWEEN '{execution_date - timedelta(days=1)}' AND '{execution_date}';",
            con=mysql_conn
        )
        print(part_df.head())

    @staticmethod
    def create_mysql_conn(mysql_conn_id: str) -> Connection:
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        return mysql_conn
