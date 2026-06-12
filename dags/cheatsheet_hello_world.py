import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Define the DAG.
with DAG(
    dag_id='cheatsheet_hello_world',
    description='A simple DAG that prints Hello, World! and the current date.',
    schedule='@daily',
    default_args={'owner': 'admin'},
    catchup=False,
    tags=['cheatsheet', 'workshop', 'ist'],
    start_date=datetime.datetime(2026, 1, 1)
) as dag:

    # Define tasks.
    say_hello = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello, World!"'
    )
    say_date = BashOperator(
        task_id='say_date',
        bash_command='date'
    )

    # Set dependencies (say_hello runs before say_date).
    say_hello >> say_date
