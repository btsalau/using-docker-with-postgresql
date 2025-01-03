"""Python ETL data pipeline to Postgres database"""

from time import time
import pandas as pd
from sqlalchemy import create_engine


df = pd.read_csv("yellow_tripdata_2019-01.csv", nrows=100)  # variable filename


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


engine = create_engine(
    "postgresql://root:root@localhost:5431/ny_taxi"
)  # variable engine, user, password, port, db_name


engine.connect()


# print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))


# print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))


df_iter = pd.read_csv("yellow_tripdata_2019-01.csv", iterator=True, chunksize=100_000)


df = next(df_iter)


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


df.head(n=0).to_sql(
    name="yellow_taxi_data", con=engine, if_exists="replace"
)  # variable table_name

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
    print("Iteration complete!")
