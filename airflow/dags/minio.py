from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import urllib.request
import os
import shutil
from minio import Minio

def grab_data_2023_to_2024():
    """Delete existing files and download files from January 2023 to February 2024 and save locally."""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    years = range(2023, 2024)
    months = range(1, 3)
    data_dir = "C:/Users/DELL/Downloads/ATL-Datamart-main/ATL-Datamart-main/data/raw"

    # Supprimer les fichiers existants dans le dossier
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    for year in years:
        for month in months:
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            file_url = base_url + filename
            output_path = os.path.join(data_dir, filename)
            try:
                urllib.request.urlretrieve(file_url, output_path)
                print(f"Downloaded {filename}")
            except Exception as e:
                print(f"Failed to download {filename}: {e}")

def write_data_minio():
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket_name = "newyork-data-bucket"
    folder_path = "C:/Users/DELL/Downloads/ATL-Datamart-main/ATL-Datamart-main/data/raw"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_name.endswith(".parquet"):
            try:
                client.fput_object(
                    bucket_name=bucket_name,
                    object_name=file_name,
                    file_path=file_path
                )
                print(f"Fichier '{file_name}' uploaded to bucket '{bucket_name}'")
            except Exception as e:
                print(f"Error uploading {file_name}: {e}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='nyc_yellow_taxi',
    default_args=default_args,
    description='DAG for downloading and uploading NYC Yellow Taxi data',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define tasks
download_task = PythonOperator(
    task_id='download_data',
    python_callable=grab_data_2023_to_2024,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=write_data_minio,
    dag=dag
)

# Set task dependencies
download_task >> upload_task
