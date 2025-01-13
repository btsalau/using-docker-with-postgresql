""" testing local ingestion with airflow"""

from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Load environment variables at the beginning of the script
load_dotenv()

# Access environment variables


url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"
directory = "/opt/airflow/downloads/output.csv"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 1, 1),
}

COMMON_ARGS = {
    "dag_id": "data_ingestion",
    "description": "An Airflow data ingestion script",
    "schedule_interval": "@monthly",
}

# Combine arguments properly
dag_args = {**COMMON_ARGS, "default_args": default_args}

with DAG(**dag_args) as dag:
    task1 = BashOperator(
        task_id="wget", bash_command=f"wget -O - {url} | gunzip > {directory}"
    )

    task2 = BashOperator(task_id="files", bash_command="ls /opt/airflow/downloads")

    task1 >> task2
