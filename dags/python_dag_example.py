from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

def greet():
    print('hello audiance, welcome to aiflow learing')


def my_biodata(name,age,location):
    print(f"hello my name is {name} , i m {age} years old and am basically from {location} ")
with DAG(
    dag_id = 'simple_python_dag',
    schedule_interval = '@daily',
    start_date = days_ago(2)
) as dag:
    task1 = PythonOperator(
        task_id ='task_1',
        python_callable = greet
    )

    task2 = PythonOperator(
        task_id = 'fnction_with_params',
        python_callable = my_biodata,
        op_kwargs = {'name':'suresh','age':24,'location':'pune'}

    )

    task1 >> task2