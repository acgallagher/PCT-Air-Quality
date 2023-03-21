from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "acgallagher",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="main_dag",
    description="This is the main DAG to run the PCT Air Quality pipeline",
    default_args=default_args,
    start_date=datetime(2023, 4, 1),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="first task", bash_command="echo hello world, this is the first task!"
    )

    task1
