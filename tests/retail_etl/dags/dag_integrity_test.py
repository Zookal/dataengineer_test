import os
import sys
from collections import namedtuple
from pathlib import Path

import pytest
from airflow.models import DAG
from airflow.utils.dag_cycle_tester import test_cycle as _test_cycle
from pytest_docker_tools import fetch, container

from tests.retail_etl.dags.helper import dag_files, import_dag_file

# NOTE: This is required to replicate how airflow manage its modules under the plugins folder.
sys.path.append(f"{Path(__file__).parent.parent.parent.parent}/retail_etl/plugins")


@pytest.fixture(scope="module")
def postgres_credentials():
    PostgresCredentials = namedtuple("PostgresCredentials", ["username", "password"])
    return PostgresCredentials("testuser", "testpass")


postgres_image = fetch(repository="postgres:13-alpine")
postgres = container(
    image="{postgres_image.id}",
    environment={
        "POSTGRES_USER": "{postgres_credentials.username}",
        "POSTGRES_PASSWORD": "{postgres_credentials.password}",
    },
    ports={"5432/tcp": None},
    volumes={
        os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        }
    },
)


@pytest.mark.parametrize("dag_file", dag_files)
class DagIntegrityTest:
    @staticmethod
    def test_dag_cycle(dag_file: str):

        module = import_dag_file(dag_file=dag_file)
        dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]

        for dag in dag_objects:
            # Test cycles
            _test_cycle(dag=dag)

        assert dag_objects

    @staticmethod
    def test_dag_default_configs(dag_file: str):
        module = import_dag_file(dag_file=dag_file)
        dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]

        assert len(dag_objects) == 1

        for dag in dag_objects:
            emails = dag.default_args.get("email", [])
            num_retries = dag.default_args.get("retries", None)
            retry_delay_sec = dag.default_args.get("retry_delay", None)
            assert emails == ["airflow@airflow.com"]
            assert num_retries is not None
            assert retry_delay_sec is not None
