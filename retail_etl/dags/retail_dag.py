from datetime import datetime, timedelta
from pathlib import Path

import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from stage_queries.helper import get_table_upsert_query
from dw_queries.helper import get_dw_table_upsert_query, get_dw_table_select_query
from custom_operators.tbl_to_staging.tbl_to_staging import TblToStageOperator
from custom_operators.postgres_dw_operator.postgres_dw_operator import PostgresDwOperator


# TODO: DRY this module


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

region_tbl_to_staging_db = TblToStageOperator(
    task_id="region_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "region.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "region",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "region",
        "upsert_query": get_table_upsert_query(table_name="region"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

nation_tbl_to_staging_db = TblToStageOperator(
    task_id="nation_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "nation.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "nation",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "nation",
        "upsert_query": get_table_upsert_query(table_name="nation"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

part_tbl_to_staging_db = TblToStageOperator(
    task_id="part_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "part.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "part",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "part",
        "upsert_query": get_table_upsert_query(table_name="part"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

supplier_tbl_to_staging_db = TblToStageOperator(
    task_id="supplier_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "supplier.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "supplier",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "supplier",
        "upsert_query": get_table_upsert_query(table_name="supplier"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

partsupp_tbl_to_staging_db = TblToStageOperator(
    task_id="partsupp_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "partsupp.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "partsupp",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "partsupp",
        "upsert_query": get_table_upsert_query(table_name="partsupp"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

customer_tbl_to_staging_db = TblToStageOperator(
    task_id="customer_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "customer.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "customer",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "customer",
        "upsert_query": get_table_upsert_query(table_name="customer"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

orders_tbl_to_staging_db = TblToStageOperator(
    task_id="orders_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "orders.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "orders",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "orders",
        "upsert_query": get_table_upsert_query(table_name="orders"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

lineitem_tbl_to_staging_db = TblToStageOperator(
    task_id="lineitem_tbl_to_staging_db",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": _SIMULATED_DATA_FOLDER / "lineitem.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
        "table_name": "lineitem",
    },
    data_load_args={
        "mysql_conn_id": "mysql_default",
        "table_name": "lineitem",
        "upsert_query": get_table_upsert_query(table_name="lineitem"),
        "logger_name": "airflow.task",
    },
    dag=dag,
)

dim_part_to_postgres_dw = PostgresDwOperator(
    task_id="dim_part_to_postgres_dw",
    pandas_read_config={
        "sql": get_dw_table_select_query(table_name="dim_part"),
        "chunksize": 10000,
        "mysql_conn_id": "mysql_default",
    },
    postgres_load_config={
        "logger_name": "airflow.task",
        "upsert_query": get_dw_table_upsert_query(table_name="dim_part"),
        "table_name": "dim_part",
        "postgres_conn_id": "postgres_default",
    },
    dag=dag,
)


dim_supplier_to_postgres_dw = PostgresDwOperator(
    task_id="dim_supplier_to_postgres_dw",
    pandas_read_config={
        "sql": get_dw_table_select_query(table_name="dim_supplier"),
        "chunksize": 10000,
        "mysql_conn_id": "mysql_default",
    },
    postgres_load_config={
        "logger_name": "airflow.task",
        "upsert_query": get_dw_table_upsert_query(table_name="dim_supplier"),
        "table_name": "dim_supplier",
        "postgres_conn_id": "postgres_default",
    },
    dag=dag,
)


dim_customer_to_postgres_dw = PostgresDwOperator(
    task_id="dim_customer_to_postgres_dw",
    pandas_read_config={
        "sql": get_dw_table_select_query(table_name="dim_customer"),
        "chunksize": 10000,
        "mysql_conn_id": "mysql_default",
    },
    postgres_load_config={
        "logger_name": "airflow.task",
        "upsert_query": get_dw_table_upsert_query(table_name="dim_customer"),
        "table_name": "dim_customer",
        "postgres_conn_id": "postgres_default",
    },
    dag=dag,
)


dim_date_to_postgres_dw = PostgresDwOperator(
    task_id="dim_date_to_postgres_dw",
    pandas_read_config={
        "sql": get_dw_table_select_query(table_name="dim_date"),
        "chunksize": 10000,
        "mysql_conn_id": "mysql_default",
    },
    postgres_load_config={
        "logger_name": "airflow.task",
        "upsert_query": get_dw_table_upsert_query(table_name="dim_date"),
        "table_name": "dim_date",
        "postgres_conn_id": "postgres_default",
    },
    dag=dag,
)


fact_lineitem_to_postgres_dw = PostgresDwOperator(
    task_id="fact_lineitem_to_postgres_dw",
    pandas_read_config={
        "sql": get_dw_table_select_query(table_name="fact_lineitem"),
        "chunksize": 10000,
        "mysql_conn_id": "mysql_default",
    },
    postgres_load_config={
        "logger_name": "airflow.task",
        "upsert_query": get_dw_table_upsert_query(table_name="fact_lineitem"),
        "table_name": "fact_lineitem",
        "postgres_conn_id": "postgres_default",
    },
    dag=dag,
)


end_execution = DummyOperator(task_id="end_execution", dag=dag)

#########################################
#           NODE CONNECTIONS            #
#########################################
begin_execution.set_downstream(region_tbl_to_staging_db)
region_tbl_to_staging_db.set_downstream(nation_tbl_to_staging_db)
nation_tbl_to_staging_db.set_downstream(
    [customer_tbl_to_staging_db, supplier_tbl_to_staging_db, part_tbl_to_staging_db]
)
customer_tbl_to_staging_db.set_downstream(orders_tbl_to_staging_db)
supplier_tbl_to_staging_db.set_downstream(partsupp_tbl_to_staging_db)
part_tbl_to_staging_db.set_downstream(partsupp_tbl_to_staging_db)
orders_tbl_to_staging_db.set_downstream(lineitem_tbl_to_staging_db)
partsupp_tbl_to_staging_db.set_downstream(lineitem_tbl_to_staging_db)
lineitem_tbl_to_staging_db.set_downstream(dim_part_to_postgres_dw)
lineitem_tbl_to_staging_db.set_downstream(dim_supplier_to_postgres_dw)
lineitem_tbl_to_staging_db.set_downstream(dim_customer_to_postgres_dw)
lineitem_tbl_to_staging_db.set_downstream(dim_date_to_postgres_dw)
dim_part_to_postgres_dw.set_downstream(fact_lineitem_to_postgres_dw)
dim_supplier_to_postgres_dw.set_downstream(fact_lineitem_to_postgres_dw)
dim_customer_to_postgres_dw.set_downstream(fact_lineitem_to_postgres_dw)
dim_date_to_postgres_dw.set_downstream(fact_lineitem_to_postgres_dw)
fact_lineitem_to_postgres_dw >> end_execution
