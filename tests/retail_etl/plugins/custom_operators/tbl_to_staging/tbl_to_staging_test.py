import logging
import os
from collections import namedtuple

from typing import Callable, Any

import MySQLdb
import pytest
from _pytest.logging import LogCaptureFixture
from MySQLdb._exceptions import ProgrammingError
from pytest_docker_tools import container, fetch
from pytest_docker_tools.wrappers import Container
from pytest_mock import MockFixture
from airflow.providers.mysql.hooks.mysql import MySqlHook

from tests.retail_etl.plugins.custom_operators.tbl_to_staging import helper


mysql_image = fetch(repository="mysql:latest")
mysql_container = container(
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


@pytest.fixture(scope="module")
def mysql_credentials():
    MySQLCredentials = namedtuple("MySQLCredentials", ["username", "password"])
    return MySQLCredentials("testuser", "testpass")


@pytest.fixture
def mysql_connection(mysql_credentials: Any, mysql_container: Container) -> MySQLdb.Connection:
    yield lambda: MySQLdb.connect(
        user=mysql_credentials.username,
        password=mysql_credentials.password,
        host="127.0.0.1",
        database="retail_test",
        port=mysql_container.ports["3306/tcp"][0],
    )


class TblToStagingTest:
    @staticmethod
    def test_operator_can_read_data_and_load_to_mysql(mocker: MockFixture, mysql_connection: Callable):
        # GIVEN
        connection = mysql_connection()
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = helper.get_valid_region_tbl_to_staging_db()

        # WHEN
        task.execute(context=None)

        # THEN
        connection = mysql_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM region;")
        result = cursor.fetchone()[0]
        assert result == 5

    @staticmethod
    def test_operator_is_resilient_to_invalid_data_format(
        mocker: MockFixture, caplog: LogCaptureFixture, mysql_connection: Callable
    ):
        # GIVEN
        connection = mysql_connection()
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = helper.get_invalid_region_tbl_to_stage_operator()

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
        mocker: MockFixture, caplog: LogCaptureFixture, mysql_connection: Callable
    ):
        # GIVEN
        connection = mysql_connection()
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = helper.get_valid_region_tbl_to_staging_db(use_valid_query=False)

        # WHEN
        logging.getLogger("airflow.task").propagate = True
        with caplog.at_level(logging.ERROR):
            task.execute(context=None)
            # THEN
            assert caplog.records[0].message == "The TblToStageOperator process has failed"
            assert isinstance(caplog.records[0].exc_info[1], ProgrammingError)
            assert str(caplog.records[0].exc_info[1]) == "not all arguments converted during bytes formatting"

    @staticmethod
    def test_operator_is_idempotent(mocker: MockFixture, mysql_connection: Callable):
        # GIVEN
        connection = mysql_connection()
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task = helper.get_valid_region_tbl_to_staging_db()

        # WHEN
        task.execute(context=None)
        connection = mysql_connection()
        mocker.patch.object(target=MySqlHook, attribute="get_conn", return_value=connection)
        task.execute(context=None)

        # THEN
        connection = mysql_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM region;")
        result = cursor.fetchone()[0]
        assert result == 5
