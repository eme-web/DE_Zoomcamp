#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'


# download the csv
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # zn = pd.read_csv('taxi+_zone_lookup.csv.1')

    # zn.to_sql(name='zones', con=engine, if_exists='replace')


    gn_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

    gn = next(gn_iter)

    gn.lpep_pickup_datetime = pd.to_datetime(gn.lpep_pickup_datetime)
    gn.lpep_dropoff_datetime = pd.to_datetime(gn.lpep_dropoff_datetime)

    gn.head(n=0).to_sql(name=table_name, con=engine,if_exists='replace')


    gn.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        
        try:

            t_start = time()
            gn = next(gn_iter)
            gn.lpep_pickup_datetime = pd.to_datetime(gn.lpep_pickup_datetime)
            gn.lpep_dropoff_datetime = pd.to_datetime(gn.lpep_dropoff_datetime)
    
            gn.to_sql(name=table_name, con=engine, if_exists='append')
    
            t_end = time()
    
            print('Inserted another chunk...,  took %.3f second' %(t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')


    args = parser.parse_args()
    main(args)



