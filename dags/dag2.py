from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'admin',
    'retries' : 5,
    'retry_delay':timedelta(minutes=2)

}


with DAG(dag_id = 'dag2',
         description = 'this is my dummy dag', 
         start_date = datetime(2025,11,20), 
         schedule_interval = '@daily',
         default_args = default_args
        ) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo "hello, lets learn Airflow..."'
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command='echo "hello, lets excute tasks"'
    )

task1 >> task2
