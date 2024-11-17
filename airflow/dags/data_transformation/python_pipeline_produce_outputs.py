import os
import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner' : 'Mustafa'
}

default_tags = ['python', 'Linked_learning', default_args['owner']]

def read_csv_file():
    df = pd.read_csv('/opt/airflow/dags/repo/airflow/dags/data_transformation/datasets/insurance.csv')
    print(df)

    return df.to_json()

def remove_null_values(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='read_csv_file'))
    df = df.dropna()
    print(df)
    
    return df.to_json()

def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def group_by_smoker(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='remove_null_values'))
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    
    output_path = '/opt/airflow/dags/data_transformation/outputs/smoker_data.csv'
    ensure_directory_exists(output_path)
    smoker_df.to_csv(output_path, index=False)
    print(smoker_df)

def group_by_region(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='remove_null_values'))
    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    
    output_path = '/opt/airflow/dags/data_transformation/outputs/region_data.csv'
    ensure_directory_exists(output_path)
    region_df.to_csv(output_path, index=False)
    print(region_df)
    
with DAG(
    dag_id = 'python_pipeline_produce_outputs',
    description = 'Read and remove null values from a CSV file and produce outputs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'data_transformation'] + default_tags
) as dag:
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv_file
    )
    
    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values
    )
    
    group_by_smoker = PythonOperator(
        task_id = 'group_by_smoker',
        python_callable = group_by_smoker
    )
    
    group_by_region = PythonOperator(
        task_id = 'group_by_region',
        python_callable = group_by_region
    )    
    
read_csv_file >> remove_null_values >> [group_by_smoker, group_by_region]