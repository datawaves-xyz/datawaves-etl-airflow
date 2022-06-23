from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'airflow_test_dag',
    default_args=default_args,
    description='Test dag',
    schedule_interval='0 0 * * *'
)

t1 = BashOperator(
    task_id='echo',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    priority_weight=2 ** 31 - 1
)
