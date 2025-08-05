import pandas as pd
import os
import sqlite3
import logging 
import time

# Setup logging
logging.basicConfig(
    filename='/Users/ashutoshraghuwanshi/Downloads/logs/ingestion_db.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Define folder path
folder = '/Users/ashutoshraghuwanshi/Downloads/data'

# Define ingestion function
def ingest_db_raw(df, table_name, conn):
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    logging.info(f"✓ Ingested {table_name} ({df.shape[0]} rows)")

# Define data loading function
def load_raw_data():
    '''This function loads CSVs as DataFrames and ingests them into DB'''
    start = time.time()
    try:
        conn = sqlite3.connect('inventory.db')
        for file in os.listdir(folder):
            if file.endswith('.csv'):
                file_path = os.path.join(folder, file)
                df = pd.read_csv(file_path)
                print(f"Loading {file}: shape {df.shape}")
                logging.info(f"Reading {file}")
                ingest_db_raw(df, file[:-4], conn)
        conn.close()
        logging.info(f"✓ All CSVs loaded in {round(time.time() - start, 2)}s")
    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

#call the function
load_raw_data()
print("All CSV files loaded successfully!")
