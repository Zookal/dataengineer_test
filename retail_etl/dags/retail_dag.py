from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from stage_queries.helper import get_table_upsert_query
from custom_operators.tbl_to_staging.tbl_to_staging import TblToStageOperator

_SIMULATED_DATA_FOLDER = Path(__file__).parent.parent.parent / "data"
_NOW = datetime.now()
_DEFAULT_ARGS = {
    "owner": "1byteyoda@makr.dev",
    "depends_on_past": False,
    "start_date": datetime(_NOW.year, _NOW.month, _NOW.day),
    "email": ["dmc.markr@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _generate_tbl_to_storage_tasks(sql_tables: List[str]):
    """TblToStageOperator factory function.

    Each tasks have a fixed batch size of 10,000 which can be modified.

    :param sql_tables: The SQL table names to be populated by the TblToStageOperator.
    :return: Iterable, which contains TblToStageOperator for each sql_tables
    """
    for table in sql_tables:
        task = TblToStageOperator(
            task_id=f"{table}_tbl_to_staging_db",
            pandas_read_args={
                # TODO: Put in Airflow Vars.
                "filepath_or_buffer": _SIMULATED_DATA_FOLDER / f"{table}.tbl",
                "chunksize": 10000,
                "sep": "|",
                "iterator": True,
                "table_name": table,
            },
            data_load_args={
                "mysql_conn_id": "mysql_default",
                "table_name": table,
                "upsert_query": get_table_upsert_query(table_name=table),
                "logger_name": "airflow.task",
            },
            dag=dag,
        )
        yield task


dag = DAG(
    dag_id="retail_dag",
    description="This DAG parses data from a set of .tbl files, stage it to a MySQL DB, then store to a DW",
    schedule_interval=timedelta(1),
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
)


#########################################
#          OPERATOR DEFINITIONS         #
#########################################
begin_execution = DummyOperator(task_id="begin_execution", dag=dag)
tbl_to_staging_db_tasks = _generate_tbl_to_storage_tasks(
    sql_tables=["region", "nation", "part", "customer", "supplier" "order", "partsupp", "lineitem"]
)
end_execution = DummyOperator(task_id="end_execution", dag=dag)


#########################################
#           NODE CONNECTIONS            #
#########################################
begin_execution.set_downstream(task_or_task_list=list(tbl_to_staging_db_tasks))
end_execution.set_upstream(task_or_task_list=list(tbl_to_staging_db_tasks))
