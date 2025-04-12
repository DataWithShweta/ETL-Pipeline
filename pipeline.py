import pandas as pd 
import os
import sqlite3
import logging
import schedule
import time
from google.cloud import storage

# setup logging 
logging.basicConfig(filename= 'logg_pipeline.log', level= logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# Set your Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\shwet\OneDrive\Desktop\Project\ETL Pipeline\learning-pipeline-key.json"

def extract_data_from_gcs(bucket_name, source_blob_name, destination_file_name ):
    try:
        storage_client = storage.Client()
        
        bucket= storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        logging.info(f"Successfully extracted {source_blob_name} from GCS.")
    except Exception as e:
        logging.error(f"Error in data extraction: {e}")
        
# extract_data_from_gcs('learning-pipeline-bucket','Storeds.csv' , './data/downloaded_Store_data.csv')        

def transform_data(file_path):
    try:
        df = pd.read_csv(file_path)
        # Remove null values
        df.dropna(inplace= True)
        # Standardizing column names
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        # Remove duplicates
        df.drop_duplicates()
        # Convert dates
        if 'date' in df.columns:
           df['date'] = pd.to_datetime(df['date'])
        
        logging.info("Data transformation successful.")
        return df
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        return None
# df = transform_data('./data/downloaded_Store_data.csv')

def load_data_to_sqlite(df, db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists= 'replace', index= False )
        conn.close()
        logging.info(f"Data successfully loaded into table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error in loading data: {e}")
        
        
def verify_data(db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name} LIMIT 5;"
        result = pd.read_sql(query, conn) 
        conn.close()
        logging.info(f"Data verification successful. Sample data: \n{result}")
    except Exception as e:
        logging.error(f"Error in verifying data: {e}")



def etl_pipeline():
    try:
        bucket_name = 'learning-pipeline-bucket'
        source_blob_name = 'Storeds.csv'
        destination_file_name = './data/downloaded_Store_data.csv'
        db_path = 'data/Store_data.db'
        table_name = 'Store_data'
        
        
        extract_data_from_gcs(bucket_name, source_blob_name , destination_file_name) 
        df = transform_data(destination_file_name)
        if df is not None:
                    load_data_to_sqlite(df, 'database.sqlite', 'Store_data')
                    verify_data('database.sqlite','Store_data' ) 
    
    except Exception as e:
       logging.error(f"ETL job failed: {e}")
    


# Schedule the ETL job
schedule.every().day.at("09:00").do(etl_pipeline)

# Run Scheduler Loop 
if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)                             
        