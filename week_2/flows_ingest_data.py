import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector




@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url):
    # download the csv
        if csv_url.endswith('.csv.gz'):
            csv_name = 'output.csv.gz'
        else:
            csv_name = 'output.csv'

        os.system(f"wget {csv_url} -O {csv_name}")

        gn_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

        gn = next(gn_iter)

        gn.lpep_pickup_datetime = pd.to_datetime(gn.lpep_pickup_datetime)
        gn.lpep_dropoff_datetime = pd.to_datetime(gn.lpep_dropoff_datetime)
        return gn

@task(log_prints=True)
def transform_data(gn):
    print(f"pre: missing passenger count: {gn['passenger_count'].isin([0]).sum()}")
    gn = gn[gn['passenger_count'] != 0]
    print(f"post: missing passenger count: {gn['passenger_count'].isin([0]).sum()}")
    return gn

@task(log_prints=True, retries=3)
def ingest_data(table_name, gn):
    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        gn.head(n=0).to_sql(name=table_name, con=engine,if_exists='replace')
        gn.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print("Logging subflow for: {table_name}")
    

@flow(name="Ingest Flow")
def main_flow(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    log_subflow(table_name) 
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)
     
if __name__ == '__main__':
    main_flow("green_taxi_trips")
  
   