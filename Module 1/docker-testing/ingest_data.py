#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # download csv
    os.system(f"curl -L --output {csv_name} {url}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    # pre-processing for yellow
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #preprocess for green
    #if green uncomment next 2 lines
    #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #create empty table
    df.head(n=0).to_sql(name=table,con=engine, if_exists='replace')
    df.to_sql(name=table, con=engine, if_exists='append')

    while True:
        try:
            df = next(df_iter)

            # if yellow uncomment next 2 lines
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            #if green uncomment next 2 lines
            #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table,con=engine, if_exists='append')

            print('chunk inserted')
        except StopIteration:
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ingest CSV data to Postgres')
    # user
    parser.add_argument('--user',help='pg user')
    # password
    parser.add_argument('--password',help='pg pass')
    # host
    parser.add_argument('--host',help='pg host')
    # port
    parser.add_argument('--port',help='pg port')
    # db name
    parser.add_argument('--db',help='pg db name')
    # tale name
    parser.add_argument('--table',help='pg table name')
    #CSV url
    parser.add_argument('--url',help='csv url')
    # parse the args
    args = parser.parse_args()

    main(args)