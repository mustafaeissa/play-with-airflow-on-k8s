import os
import logging
import pandas as pd
import pyarrow.parquet as pq

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = "de-learning-442711"
BUCKET ="dez_learning_bucket"

# dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = "yellow_tripdata_2024-09.parquet"
BIGQUERY_DATASET = "DEZ_learning_dataset_112024"

# def download_dataset(ti):
    # logging.info(f"Downloading dataset from {dataset_url}")
    # os.system(f"curl -sSL {dataset_url} -o /tmp/dataset.parquet")
    # logging.info(f"Dataset downloaded to /tmp/dataset.parquet")
    # df_par = pd.read_parquet("/tmp/dataset.parquet", engine='pyarrow')
    # ti.xcom_push(key='data', value=df_par.to_json())
    # logging.info(f"Dataset pushed to Xcom")

    
# def format_to_parquet(ti):
#     logging.info("Pulling XCom value from download_dataset_task")
#     src_file = ti.xcom_pull(task_ids="download_dataset_task")
#     logging.info(f"Source file: {src_file}")
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace('.csv', '.parquet'))
#     logging.info(f"File converted to Parquet: {src_file.replace('.csv', '.parquet')}")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def download_and_upload_dataset(bucket, object_name):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    
    logging.info(f"Downloading dataset from {dataset_url}")
    os.system(f"curl -sSL {dataset_url} -o /tmp/dataset.parquet")
    logging.info(f"Dataset downloaded to /tmp/dataset.parquet")
    
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename('/tmp/dataset.parquet')
    logging.info(f"Dataset uploaded to GCS: gs://{bucket}/{object_name}")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de', 'zoomcamp'],
) as dag:

    # download_dataset_task = PythonOperator(
    #     task_id="download_dataset_task",
    #     python_callable=download_dataset
    # )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet
    # )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    download_and_upload_dataset_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=download_and_upload_dataset,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            # "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_and_upload_dataset_task >> bigquery_external_table_task