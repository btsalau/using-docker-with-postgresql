""" Properly defining the URL for the ingestion script to run monthly
URL_TEMPLATE = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
version_date = datetime.now().strftime("%Y-%m")
FULL_URL = URL_TEMPLATE + '/yellow_tripdata_' + version_date + '.csv.gz'
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


def test_connection(
    DB_ENGINE: str,
    DB_USER: str,
    DB_PASSWORD: str,
    DB_HOST: str,
    DB_PORT: str,
    DB_NAME: str,
) -> str:
    """Test connection to the app Postgres database"""
    try:
        engine = create_engine(
            f"{DB_ENGINE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        engine.connect()
    except Exception as e:
        return str(e)
    return "Connection successful"


# Load environment variables at the beginning of the script
load_dotenv()

# Access environment variables
db_engine, user, password, host, port, db_name = (
    os.getenv("DB_ENGINE"),
    os.getenv("DB_USER"),
    os.getenv("DB_PASSWORD"),
    os.getenv("DB_HOST"),
    os.getenv("DB_PORT"),
    os.getenv("DB_NAME"),
)

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"

DIRECTORY = "/opt/airflow/downloads/output.csv"

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
        task_id="wget", bash_command=f"wget -O - {URL} | gunzip > {DIRECTORY}"
    )

    task2 = BashOperator(task_id="files", bash_command="ls /opt/airflow/downloads")

    task3 = PythonOperator(
        task_id="engine_connection",  # task identifier
        python_callable=test_connection,  # python function
        op_kwargs={
            "DB_ENGINE": db_engine,
            "DB_USER": user,
            "DB_PASSWORD": password,
            "DB_HOST": host,
            "DB_PORT": port,
            "DB_NAME": db_name,
        },  # function parameters and arguments
    )

    task1 >> task2 >> task3
