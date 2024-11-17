from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'Mustafa'
}

default_tags = ['python', 'mustafa', 'Linked_learning']

def print_function():
    print("The simplest possible Python operator!")
    
with DAG(
    dag_id = 'simple_python_operator',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = '@once',
    tags = ['simple', 'python'] + default_tags
) as dag:
    task = PythonOperator(
        task_id = 'python_task',
        python_callable = print_function
    )
    
task