"""Dockerised Python ETL script to Postgres database"""

import os
from time import time
import argparse
from typing import Any
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()


def ingest(variables: Any) -> None:
    """Function for running ETL script"""

    csv_file = variables.csv
    db_engine = variables.engine
    user = variables.user
    password = variables.password
    port = variables.port
    db_name = variables.db
    table = variables.table
    host = variables.host

    command = f"wget -O - {variables.url} | gunzip > {csv_file}"
    os.system(command)

    # print(db_engine, user, password, host, port, db_name)

    engine = create_engine(f"{db_engine}://{user}:{password}@{host}:{port}/{db_name}")

    engine.connect()

    # print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))

    # print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))

    df_iter = pd.read_csv(f"{csv_file}", iterator=True, chunksize=100_000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")

    df.to_sql(name=table, con=engine, if_exists="replace")

    try:
        while True:
            t_start = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table, con=engine, if_exists="append")

            t_end = time()

            t_time = t_end - t_start

            print(f"inserted another chunk... Took {t_time:.3f} seconds!")

    except StopIteration:
        print("Iteration complete!")

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="environment variables for ETL script. Default values are in .env file"
    )

    parser.add_argument("url", help="URL to download CSV")

    parser.add_argument(
        "-c", "--csv", default=os.getenv("CSV_FILE"), help="Path of CSV file"
    )
    parser.add_argument(
        "-e", "--engine", default=os.getenv("ENGINE"), help="DB engine to connect"
    )
    parser.add_argument("-u", "--user", default=os.getenv("USER"), help="User name")
    parser.add_argument(
        "-p", "--password", default=os.getenv("PASSWORD"), help="User password"
    )
    parser.add_argument("--host", default=os.getenv("HOST"), help="Host")
    parser.add_argument("--port", default=os.getenv("PORT"), help="Port", type=int)
    parser.add_argument(
        "-d", "--db", default=os.getenv("DB_NAME"), help="Name of database"
    )
    parser.add_argument(
        "-t", "--table", default=os.getenv("TABLE"), help="Name of table"
    )

    variables = parser.parse_args()

    ingest(variables)
