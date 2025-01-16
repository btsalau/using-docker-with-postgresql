""" Properly defining the URL & OUTPUT PATH for the ingestion script to run monthly

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
version_date = datetime.now().strftime("%Y-%m")
FULL_URL = URL_PREFIX + '/yellow_tripdata_' + version_date + '.csv.gz'
DIRECTORY = "/opt/airflow/downloads/"
FILE_PATH = f'{DIRECTORY}/yellow_tripdata_{version_date}.csv'
"""

import os
from datetime import datetime, timedelta
from time import time

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine


def test_connection(
    DB_ENGINE: str,
    DB_USER: str,
    DB_PASSWORD: str,
    DB_HOST: str,
    DB_PORT: str,
    DB_NAME: str,
) -> dict[str, str]:
    """Test connection to the app Postgres database"""
    try:
        engine = create_engine(
            f"{DB_ENGINE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        engine.connect()
    except Exception as e:
        return str(e)
    connection_parameters = {
        "DB_ENGINE": DB_ENGINE,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_NAME": DB_NAME,
    }
    print("Connection successful")
    return connection_parameters


def process_file(ti: dict, file_path: str, table: str) -> None:
    """Process the downloaded file"""

    connection_parameters = ti.xcom_pull(task_ids="engine_connection")

    engine = create_engine(
        f"{connection_parameters['DB_ENGINE']}://{connection_parameters['DB_USER']}:{connection_parameters['DB_PASSWORD']}@{connection_parameters['DB_HOST']}:{connection_parameters['DB_PORT']}/{connection_parameters['DB_NAME']}"
    )
    engine.connect()

    df_iter = pd.read_csv(f"{file_path}", iterator=True, chunksize=100_000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")

    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")

    try:
        while True:
            t_start = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

            t_end = time()

            t_time = t_end - t_start

            print(f"inserted another chunk...{t_time:.3f} seconds")

    except StopIteration:
        return "Iteration complete!"


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

    task2 = BashOperator(task_id="check_file", bash_command="ls /opt/airflow/downloads")

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

    task4 = PythonOperator(
        task_id="process_data",  # task identifier
        python_callable=process_file,  # python function
        op_kwargs={
            "file_path": DIRECTORY,
            "table": "yellow_taxi_trips",
        },  # function parameters and arguments
    )

    task1 >> task2 >> task3 >> task4
