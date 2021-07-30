from typing import Dict

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


class DataSyncOperator(BaseOperator):
    def __init__(self, mysql_conn_id: str, insert_sql_query: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.insert_sql_query = insert_sql_query

    def execute(self, context: Dict):
        self.log.info("DataSyncOperator Starting...")
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()

        mysql_cursor.execute(query="SELECT * FROM REGION;")
        res = mysql_cursor.fetchall()
        print(res)
