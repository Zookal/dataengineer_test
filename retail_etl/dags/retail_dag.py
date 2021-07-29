import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator


dag = DAG(
    dag_id="retail_dag",
    schedule_interval="*/15 * * * *",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
)


begin_execution = DummyOperator(task_id="begin_execution", dag=dag)


end_execution = DummyOperator(task_id="end_execution", dag=dag)


begin_execution >> end_execution
