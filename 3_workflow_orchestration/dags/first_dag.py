""" testing airflow dag"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "first_dag",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id="print_date",
    bash_command="date",
    dag=dag,
)

t2 = BashOperator(
    task_id="sleep",
    bash_command="sleep 5",
    retries=3,
    dag=dag,
)

t3 = BashOperator(
    task_id="echo_hello",
    bash_command='echo "Hello World!"',
    dag=dag,
)

t1 >> t2 >> t3
