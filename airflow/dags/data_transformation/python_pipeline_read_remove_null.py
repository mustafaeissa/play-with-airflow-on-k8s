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
    df = pd.read_csv('/opt/airflow/dags/data_transformation/datasets/data.csv')
    print(df)

    return df.to_json()

def remove_null_values(**kwargs):
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(task_ids='read_csv_file'))
    df = df.dropna()
    print(df)
    
    return df.to_json()    

with DAG(
    dag_id = 'python_pipeline_read_remove_null',
    description = 'Read and remove null values from a CSV file',
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
    
    read_csv_file >> remove_null_values