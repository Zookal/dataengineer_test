from typing import Dict

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


class DataSyncOperator(BaseOperator):
    def __init__(self, mysql_conn_id: str, create_table_script: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.create_table_script = create_table_script

    def execute(self, context: Dict):
        self.log.info("DataSyncOperator Starting...")
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()

        mysql_cursor.execute(query="SELECT * FROM REGION;")
        res = mysql_cursor.fetchall()
        print(res)
