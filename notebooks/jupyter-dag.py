from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    dag_id='my_dag',
    schedule_interval='0 0 * * *',  # Runs once daily at midnight
    start_date=datetime(2023, 5, 29),
    catchup=False
)

# Define tasks
task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Task 1"',
    dag=dag
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "Task 2"',
    dag=dag
)

# Define task dependencies
task1 >> task2
