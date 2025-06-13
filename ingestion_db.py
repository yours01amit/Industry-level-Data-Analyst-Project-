import pandas as pd
import os
from sqlalchemy import create_engine
import time

import logging

logging.basicConfig(
  filename='logs/ingestion_db.log',
  level=logging.DEBUG,
  format="%(asctime)s - %(levelname)s - %(message)s",
  filemode='a'
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe into database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
  

def load_raw_data():
    '''This function will load the the csv files as dataframes and ingest data into DB'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
      
    end = time.time()
    total_time = (end-start)/60
    logging.info('Ingestion completed')
    
    logging.info(f'\nTotal Time Taken: {total_time} mins')
    
if __name__ == "__main__":
    load_raw_data()
