from collections import Counter
from typing import Dict

import pytest
from airflow.models import DAG

from tests.retail_etl.dags.helper import dag_files, import_dag_file, get_retail_dag_task_hierarchy


class RetailDagTaskDefTest:
    EXPECTED_TASKS_COUNT = 10
    DAG_ID = "retail_dag"
    EXPECTED_TASKS = [
        "begin_execution",
        "region_tbl_to_staging_db",
        "nation_tbl_to_staging_db",
        "part_tbl_to_staging_db",
        "supplier_tbl_to_staging_db",
        "partsupp_tbl_to_staging_db",
        "customer_tbl_to_staging_db",
        "orders_tbl_to_staging_db",
        "lineitem_tbl_to_staging_db",
        "end_execution",
    ]

    @staticmethod
    def test_dag_task_count_is_correct(retail_dag: DAG):
        tasks_count = len(retail_dag.tasks)
        msg = f"Wrong number of tasks, got {tasks_count}"
        assert tasks_count == RetailDagTaskDefTest.EXPECTED_TASKS_COUNT, msg

    @staticmethod
    def test_dag_contains_valid_tasks(retail_dag: DAG):
        task_ids = list(map(lambda task: task.task_id, retail_dag.tasks))
        assert RetailDagTaskDefTest._compare_tasks(task_ids, RetailDagTaskDefTest.EXPECTED_TASKS)

    @pytest.mark.parametrize("task", get_retail_dag_task_hierarchy())
    def test_dependencies_of_tasks(self, retail_dag: DAG, task: Dict):
        expected_upstream = task.get("expected_upstream")
        expected_downstream = task.get("expected_downstream")
        task_name = task.get("task")
        dag_task = retail_dag.get_task(task_name)

        upstream_msg = f"The task {task} doesn't have the expected " "upstream dependencies."
        downstream_msg = f"The task {task} doesn't have the expected " "downstream dependencies."
        assert self._compare_tasks(task_a=dag_task.upstream_task_ids, task_b=expected_upstream), upstream_msg
        assert self._compare_tasks(
            task_a=dag_task.downstream_task_ids, task_b=expected_downstream
        ), downstream_msg

    @staticmethod
    def test_dag_will_not_perform_catchup(retail_dag: DAG):
        catchup = retail_dag.catchup
        assert not catchup

    @staticmethod
    def test_dag_have_same_start_date_for_each_tasks(retail_dag: DAG):
        tasks = retail_dag.tasks
        start_dates = list(map(lambda task: task.start_date, tasks))
        assert len(set(start_dates)) == 1

    @classmethod
    @pytest.fixture(scope="class")
    def retail_dag(cls):
        for dag_file in dag_files:
            module = import_dag_file(dag_file=dag_file)
            dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
            dag = list(filter(lambda _dag: _dag.dag_id == cls.DAG_ID, dag_objects))[0]
            return dag

    @staticmethod
    def _compare_tasks(task_a, task_b):
        return Counter(task_a) == Counter(task_b)
