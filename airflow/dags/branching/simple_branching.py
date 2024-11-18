import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from random import choice


default_args = {
    'owner' : 'Mustafa'
}

default_tags = ['python', 'Linked_learning', default_args['owner']]

def has_driving_license():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):
        ## the branch function returns a the task ID as string which is used by the BranchPythonOperator to decide which task to run next ##
        return 'eligible_to_drive'
    else:
        return 'not_eligible_to_drive'                      

def eligible_to_drive():
    print("You can drive, you have a license!")

def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need a license to drive")
    
with DAG(
    'simple_branching',
    description = 'A simple branching DAG',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = '@once',
    tags = ['branching'] + default_tags
) as dag:
        
        has_driving_license = PythonOperator(
            task_id = 'has_driving_license',
            python_callable = has_driving_license,
        )
        
        branch = BranchPythonOperator(
            task_id = 'branch',
            python_callable = branch,
            provide_context = True,
        )
        
        eligible_to_drive = PythonOperator(
            task_id = 'eligible_to_drive',
            python_callable = eligible_to_drive,
        )
        
        not_eligible_to_drive = PythonOperator(
            task_id = 'not_eligible_to_drive',
            python_callable = not_eligible_to_drive,
        )
        
        has_driving_license >> branch >> [eligible_to_drive, not_eligible_to_drive]