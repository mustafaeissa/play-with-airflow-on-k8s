from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner' : 'Mustafa',
}

## Define the DAG as a vaiable called dag and pass the DAG into the operator ##
dag = DAG(
    dag_id = 'hello_world_v2',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = '@once',
    tags = ['beginner', 'bash', 'hello world', 'mustafa', 'Linked_learning']
)

task = BashOperator(
    task_id = 'hello_world_task',
    bash_command = 'echo Hello world once again!',
    dag = dag
)

task
