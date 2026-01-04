from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago


def decide_func_to_be_executed():
    val = 5
    if val < 10:
        return "task3"
    else:
        return "task4"
    
def smaller():
    print(' my value is 4')

def greater():
    print(' my value is 14')

with DAG(
    dag_id = 'python_branch_operator_ex',
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    task1 = PythonOperator(
        task_id = 'start',
        python_callable = lambda  : print('starting')
    )

    task2 = BranchPythonOperator(
        task_id = 'branching_task',
        python_callable = decide_func_to_be_executed
    )

    task3 = PythonOperator(
        task_id = 'task3',
        python_callable = greater
    )
    task4 = PythonOperator(
        task_id = 'task4',
        python_callable = smaller
    )

    task1 >> task2 >> [task3,task4] 