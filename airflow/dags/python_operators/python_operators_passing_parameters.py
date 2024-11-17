from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'Mustafa'
}

default_tags = ['python', 'mustafa', 'Linked_learning']

def greet_hello(name):
    print(f"Hello {name}!")
    
def greet_hello_with_city(name, city):
    print(f"Hello {name} from {city}!")
    
with DAG(
    dag_id = 'passing_param_python_operators',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = '@once',
    tags = ['parameters', 'python'] + default_tags
) as dag:
    taskA = PythonOperator(
        task_id = 'greet_hello',
        python_callable = greet_hello,
        op_kwargs = {'name': 'Mustafa'}
    )
    
    taskB = PythonOperator(
        task_id = 'greet_hello_with_city',
        python_callable = greet_hello_with_city,
        op_kwargs = {'name': 'Mustafa', 'city': 'Krakow'}
    )
    
taskA >> taskB