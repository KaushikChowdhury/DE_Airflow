#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    
    sql_engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db_name}')
    sql_engine.connect()

    csv_name = 'output.csv'

    os.system(f'wget {params.url} -O {csv_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)


    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.passenger_count = pd.to_numeric(df.passenger_count)
    df.trip_distance = pd.to_numeric(df.trip_distance)

    df.head(0).to_sql(name=f'{params.table}', con= sql_engine, if_exists='replace')
    df.to_sql(name=f'{params.table}', con= sql_engine, if_exists='append')


    while True:
        
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.passenger_count = pd.to_numeric(df.passenger_count)
        df.trip_distance = pd.to_numeric(df.trip_distance)
        
        df.to_sql(name=f'{params.table}', con= sql_engine, if_exists='append')
        
        print('inserted another chunk')


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')
    parser.add_argument('--user',help='user name for postgres database')
    parser.add_argument('--password',help='password for postgres database')
    parser.add_argument('--host',help='hostname for postgres database')
    parser.add_argument('--port',help='port for postgres database')
    parser.add_argument('--db_name',help='postgres database name')
    parser.add_argument('--table',help='table for postgres database')
    parser.add_argument('--url',help='url for csv file')

    args = parser.parse_args()

    print(args)
    # print(params.password)

    main(args)
