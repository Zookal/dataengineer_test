from pathlib import Path

import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from custom_operators.tbl_to_staging.tbl_to_staging import TblToStageOperator


dag = DAG(
    dag_id="retail_dag",
    schedule_interval=airflow.utils.dates.timedelta(1),
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
)

begin_execution = DummyOperator(task_id="begin_execution", dag=dag)

region_tbl_to_staging_db = TblToStageOperator(
    task_id="region_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="REGION",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "region.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)


nation_tbl_to_staging_db = TblToStageOperator(
    task_id="nation_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="NATION",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "nation.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)


part_tbl_to_staging_db = TblToStageOperator(
    task_id="part_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="NATION",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "part.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)


customer_tbl_to_staging_db = TblToStageOperator(
    task_id="customer_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="CUSTOMER",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "customer.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)

supplier_tbl_to_staging_db = TblToStageOperator(
    task_id="supplier_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="SUPPLIER",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "supplier.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)

order_tbl_to_staging_db = TblToStageOperator(
    task_id="order_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="ORDER",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "orders.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)

partsupp_tbl_to_staging_db = TblToStageOperator(
    task_id="partsupp_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="PARTSUPP",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "partsupp.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)

lineitem_tbl_to_staging_db = TblToStageOperator(
    task_id="lineitem_tbl_to_staging_db",
    mysql_conn_id="mysql_default",
    table_name="LINEITEM",
    pandas_read_args={
        # TODO: Put in Airflow Vars.
        "filepath_or_buffer": Path(__file__).parent.parent.parent / "data" / "lineitem.tbl",
        "chunksize": 10000,
        "sep": "|",
        "iterator": True,
    },
    dag=dag,
)

end_execution = DummyOperator(task_id="end_execution", dag=dag)

tbl_to_staging_db_tasks = [
    region_tbl_to_staging_db,
    nation_tbl_to_staging_db,
    part_tbl_to_staging_db,
    customer_tbl_to_staging_db,
    supplier_tbl_to_staging_db,
    order_tbl_to_staging_db,
    partsupp_tbl_to_staging_db,
    lineitem_tbl_to_staging_db
]

begin_execution.set_upstream(task_or_task_list=tbl_to_staging_db_tasks)
end_execution.set_downstream(task_or_task_list=tbl_to_staging_db_tasks)
