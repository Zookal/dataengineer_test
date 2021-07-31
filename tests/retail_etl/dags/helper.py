import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import List, Dict

_dag_path = Path(__file__).parent.parent.parent.parent / "retail_etl" / "dags"
dag_files = list(_dag_path.glob("**/retail_dag.py"))


def get_retail_dag_task_hierarchy() -> List[Dict]:
    return [
        {
            "task": "begin_execution",
            "expected_upstream": [],
            "expected_downstream": ["region_tbl_to_staging_db"],
        },
        {
            "task": "region_tbl_to_staging_db",
            "expected_upstream": ["begin_execution"],
            "expected_downstream": ["end_execution"],
        },
        {
            "task": "end_execution",
            "expected_upstream": ["region_tbl_to_staging_db"],
            "expected_downstream": [],
        },
    ]


def import_dag_file(dag_file: str) -> ModuleType:
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(_dag_path, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    return module
