import datetime

from pathlib import Path

from airflow.models import DAG

from retail_etl.plugins.custom_operators.tbl_to_staging.tbl_to_staging import TblToStageOperator
from retail_etl.plugins.stage_queries import region as region_tbl_queries


def test_dag():
    return DAG(
        dag_id="test_dag",
        default_args={"owner": "makr.dev", "start_date": datetime.datetime(2015, 1, 1)},
        schedule_interval="@daily",
    )


def get_valid_region_tbl_to_staging_db(use_valid_query: bool = True) -> TblToStageOperator:
    upsert_query = region_tbl_queries.get_upsert_query() if use_valid_query else "INSERT INTO ... ..."
    return TblToStageOperator(
        task_id="region_tbl_to_staging_db",
        pandas_read_args={
            # TODO: Put in Airflow Vars.
            "filepath_or_buffer": Path(__file__).parent / "test_data" / "region.tbl",
            "chunksize": 10000,
            "sep": "|",
            "iterator": True,
            "table_name": "region",
        },
        data_load_args={
            "mysql_conn_id": "mysql",
            "table_name": "region",
            "upsert_query": upsert_query,
            "logger_name": "airflow.task",
        },
        dag=test_dag(),
    )


def get_invalid_region_tbl_to_stage_operator() -> TblToStageOperator:
    upsert_query = region_tbl_queries.get_upsert_query()
    return TblToStageOperator(
        task_id="region_tbl_to_staging_db",
        pandas_read_args={
            # TODO: Put in Airflow Vars.
            "filepath_or_buffer": Path(__file__).parent / "test_data" / "invalid_format_region.tbl",
            "chunksize": 10000,
            "sep": "|",
            "iterator": True,
            "table_name": "region",
        },
        data_load_args={
            "mysql_conn_id": "mysql",
            "table_name": "region",
            "upsert_query": upsert_query,
            "logger_name": "airflow.task",
        },
        dag=test_dag(),
    )
