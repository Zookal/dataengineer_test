import datetime
import logging
import os
from pathlib import Path
from collections import namedtuple
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
import MySQLdb
from MySQLdb._exceptions import ProgrammingError
from pytest_docker_tools import fetch, container
from pytest_mock import MockFixture
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import DAG

from retail_etl.plugins.custom_operators.tbl_to_staging.tbl_to_staging import TblToStageOperator
from retail_etl.plugins.stage_queries import region as region_tbl_queries


@pytest.fixture
def test_dag():
    return DAG(
        dag_id="test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2015, 1, 1)},
        schedule_interval="@daily",
    )


@pytest.fixture(scope="module")
def mysql_credentials():
    MySQLCredentials = namedtuple("MySQLCredentials", ["username", "password"])
    return MySQLCredentials("testuser", "testpass")


mysql_image = fetch(repository="mysql:latest")
mysql_session = container(
    image="{mysql_image.id}",
    environment={
        "MYSQL_DATABASE": "retail_test",
        "MYSQL_USER": "{mysql_credentials.username}",
        "MYSQL_PASSWORD": "{mysql_credentials.password}",
        "MYSQL_ROOT_PASSWORD": "{mysql_credentials.password}",
    },
    ports={"3306/tcp": None},
    volumes={
        os.path.join(os.path.dirname(__file__), "test_data/ddl_test.sql"): {
            "bind": "/docker-entrypoint-initdb.d/ddl_test.sql"
        }
    },
)


class TblToStagingTest:
    @staticmethod
    def test_operator_can_read_data_and_load_to_mysql(
        mysql_credentials: Any, mocker: MockFixture, mysql_session, test_dag: DAG
    ):
        # GIVEN
        connection = MySQLdb.connect(
            user=mysql_credentials.username,
            password=mysql_credentials.password,
            host="127.0.0.1",
            database="retail_test",
            port=mysql_session.ports["3306/tcp"][0],
        )
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = TblToStageOperator(
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
                "upsert_query": region_tbl_queries.get_upsert_query(),
                "logger_name": "airflow.task",
            },
            dag=test_dag,
        )

        # WHEN
        task.execute(context=None)

        # THEN
        connection = MySQLdb.connect(
            user=mysql_credentials.username,
            password=mysql_credentials.password,
            host="127.0.0.1",
            database="retail_test",
            port=mysql_session.ports["3306/tcp"][0],
        )

        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM region;")
        result = cursor.fetchone()[0]
        assert result == 5

    @staticmethod
    def test_operator_is_resilient_to_invalid_data_format(
        mysql_credentials: Any, mocker: MockFixture, mysql_session, test_dag: DAG, caplog: LogCaptureFixture
    ):
        # GIVEN
        connection = MySQLdb.connect(
            user=mysql_credentials.username,
            password=mysql_credentials.password,
            host="127.0.0.1",
            database="retail_test",
            port=mysql_session.ports["3306/tcp"][0],
        )
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = TblToStageOperator(
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
                "upsert_query": region_tbl_queries.get_upsert_query(),
                "logger_name": "airflow.task",
            },
            dag=test_dag,
        )

        # WHEN
        logging.getLogger("airflow.task").propagate = True
        with caplog.at_level(logging.ERROR):
            task.execute(context=None)
            # THEN
            assert caplog.records[0].message == "The TblToStageOperator process has failed"
            assert isinstance(caplog.records[0].exc_info[1], ProgrammingError)
            assert str(caplog.records[0].exc_info[1]) == "nan can not be used with MySQL"

    @staticmethod
    def test_operator_is_resilient_to_invalid_sql_query(
        mysql_credentials: Any, mocker: MockFixture, mysql_session, test_dag: DAG, caplog: LogCaptureFixture
    ):
        # GIVEN
        connection = MySQLdb.connect(
            user=mysql_credentials.username,
            password=mysql_credentials.password,
            host="127.0.0.1",
            database="retail_test",
            port=mysql_session.ports["3306/tcp"][0],
        )
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = TblToStageOperator(
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
                "upsert_query": "INSERT INTO ... INVALID ...",
                "logger_name": "airflow.task",
            },
            dag=test_dag,
        )

        # WHEN
        logging.getLogger("airflow.task").propagate = True
        with caplog.at_level(logging.ERROR):
            task.execute(context=None)
            # THEN
            assert caplog.records[0].message == "The TblToStageOperator process has failed"
            assert isinstance(caplog.records[0].exc_info[1], ProgrammingError)
            assert str(caplog.records[0].exc_info[1]) == "not all arguments converted during bytes formatting"
