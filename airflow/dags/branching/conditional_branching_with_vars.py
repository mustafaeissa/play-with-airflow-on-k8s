import os
import pandas as pd

import pendulum
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator



default_args = {
    'owner' : 'Mustafa'
}

default_tags = ['python', 'Linked_learning', default_args['owner']]

DATASETS_PATH = '/opt/airflow/dags/repo/airflow/dags/branching/datasets/insurance.csv'
OUTPUT_PATH  = '/opt/airflow/dags/repo/airflow/dags/branching/outputs/{0}.csv'

def read_csv_file():
    df = pd.read_csv('/opt/airflow/dags/repo/airflow/dags/branching/datasets/insurance.csv')
    print(df)

    return df.to_json()

def remove_null_values(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='read_csv_file'))
    df = df.dropna()
    print(df)
    
    return df.to_json()

# def ensure_directory_exists(file_path):
#     directory = os.path.dirname(file_path)
#     if not os.path.exists(directory):
#         os.makedirs(directory)

def ensure_directory_exists(file_path, dir_permissions=0o755, owner='airflow', group='airflow'):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        os.chmod(directory, dir_permissions)
        os.chown(directory, os.getuid(), os.getgid())  # Change ownership to the specified user and group
    
    # # Ensure the file has the correct permissions if it already exists
    # if os.path.exists(file_path):
    #     os.chmod(file_path, 0o644)
    #     os.chown(file_path, os.getuid(), os.getgid())  # Change ownership to the specified user and group
        
def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)
    
    if transform_action.startswith('filter'):
        return transform_action
    elif transform_action == 'groupby_region_smoker':
        return 'groupby_region_smoker'


def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    ensure_directory_exists(OUTPUT_PATH)
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    ensure_directory_exists(OUTPUT_PATH)
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    ensure_directory_exists(OUTPUT_PATH)
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    ensure_directory_exists(OUTPUT_PATH)
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    ensure_directory_exists(OUTPUT_PATH)
    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)

    
with DAG(
    dag_id = 'executing_branching',
    description = 'Running a branching pipeline to filter data based vars',
    default_args = default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule = '@once',
    tags = ['python', 'data_transformation', 'pipeline', 'branching'] + default_tags
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_southwest = PythonOperator(
        task_id='filter_by_southwest',
        python_callable=filter_by_southwest
    )
    
    filter_by_southeast = PythonOperator(
        task_id='filter_by_southeast',
        python_callable=filter_by_southeast
    )

    filter_by_northwest = PythonOperator(
        task_id='filter_by_northwest',
        python_callable=filter_by_northwest
    )

    filter_by_northeast = PythonOperator(
        task_id='filter_by_northeast',
        python_callable=filter_by_northeast
    )

    groupby_region_smoker = PythonOperator(
        task_id='groupby_region_smoker',
        python_callable=groupby_region_smoker
    )
    
    read_csv_file >> remove_null_values >> determine_branch >> [filter_by_southwest, 
                                                                filter_by_southeast, 
                                                                filter_by_northwest,
                                                                filter_by_northeast,
                                                                groupby_region_smoker]
