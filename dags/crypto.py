from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import requests
import json
import csv
from datetime import datetime
from google.cloud import storage
import os

# Set up default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 16),
    'retries': 1,
}

# Define DAG
with DAG('coingecko_gcp_dag',
         default_args=default_args,
         schedule_interval='@daily',  # Runs every day
         catchup=False) as dag:
    
    # Function to fetch data from CoinGecko API and save as JSON in GCS
    def fetch_coingecko_data():
        try:
            url = 'https://api.coingecko.com/api/v3/coins/markets'
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 100,
                'page': 1,
                'sparkline': 'false'
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Save raw JSON data to GCS
            client = storage.Client()
            bucket = client.get_bucket('crypto-bucket-ravi9')  
            blob = bucket.blob('coingecko/raw_data.json')
            blob.upload_from_string(json.dumps(data), content_type='application/json')
        except Exception as e:
            print(f"Error fetching or uploading data: {e}")
            raise

    # Function to transform JSON data to CSV and store in GCS
    def transform_data_to_csv():
        try:
            client = storage.Client()
            bucket = client.get_bucket('crypto-bucket-ravi9')  
            blob = bucket.blob('coingecko/raw_data.json')
            raw_data = json.loads(blob.download_as_text())

            # Transform raw JSON data to CSV
            csv_file_path = '/tmp/transformed_data.csv'
            with open(csv_file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                # Write headers
                writer.writerow(['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume'])
                # Write rows
                for coin in raw_data:
                    writer.writerow([coin['id'], coin['symbol'], coin['name'], coin['current_price'], coin['market_cap'], coin['total_volume']])

            # Save transformed CSV to GCS
            bucket = client.get_bucket('crypto-bucket-transformed-ravi9')  
            blob = bucket.blob('coingecko/transformed_data.csv')
            blob.upload_from_filename(csv_file_path)

            # Clean up local temp file
            os.remove(csv_file_path)

        except Exception as e:
            print(f"Error transforming or uploading data: {e}")
            raise

    # Fetch raw data from CoinGecko API
    fetch_data = PythonOperator(
        task_id='fetch_coingecko_data',
        python_callable=fetch_coingecko_data
    )

    # Transform raw data to CSV
    transform_data = PythonOperator(
        task_id='transform_data_to_csv',
        python_callable=transform_data_to_csv
    )

    # Load transformed CSV data to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='crypto-bucket-transformed-ravi9',  
        source_objects=['coingecko/transformed_data.csv'],
        destination_project_dataset_table='write the table id',  
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )

    # Define task dependencies
    fetch_data >> transform_data >> load_to_bigquery
