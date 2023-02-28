#!/usr/bin/env python
# coding: utf-8
import argparse
import pandas as pd
import time
from sqlalchemy import create_engine
import os

def main(params):
    
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    parquet_name = 'output.parquet'
    os.system(f'wget {url} -O {parquet_name}')
    df_pq = pd.read_parquet(parquet_name, engine = 'pyarrow')
    
    csv_data_source = 'yellow_tripdata_2022-01.csv'
    df_pq.to_csv(csv_data_source, index = False, sep='\t')
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # Check the schema for this data set
    df = pd.read_csv(csv_data_source, sep='\t', nrows=100)
    print('Show the table schema before transformation: \n',pd.io.sql.get_schema(df, name = table_name, con=engine))
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print('Show the table schema after transformation: \n',pd.io.sql.get_schema(df, name = table_name, con=engine))

    # Create table 
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df_iter = pd.read_csv(csv_data_source, sep='\t', dtype={'store_and_fwd_flag': str}, iterator=True, chunksize=500000)
    loop = True
    while loop: 
        try:
            t_start = time.time()
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time.time()
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            loop = False
            print('Iteration is END!!!')

    '''# TEST database
    query = f"""
    SELECT * FROM {table_name} LIMIT 10
    """
    pd.read_sql(query, con=engine)
    '''
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')
    # user, password, host, port, database name, table name, url of the parquet file
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port name for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')
    
    args = parser.parse_args()
    main(args)