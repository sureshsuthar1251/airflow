from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago


def getname():
    return 'Rohit Sharma'

def info(ti):
    ti.xcom_push(key = 'age',value = '24')
    ti.xcom_push(key = 'education',value = 'BCA')
    ti.xcom_push(key = 'company',value = 'Tredence')
    ti.xcom_push(key = 'designation',value = 'Data Engineer')
    


def getIntro(ti):
    # get only a single value, dont need to give any key,
    name = ti.xcom_pull(task_ids = 'get_name')
    age = ti.xcom_pull(task_ids = 'info',key = 'age')
    education = ti.xcom_pull(task_ids = 'info',key = 'education')
    company = ti.xcom_pull(task_ids = 'info',key = 'company')
    designation = ti.xcom_pull(task_ids = 'info',key = 'designation')
    
    return f"I'm {name} and my age is {age} and im {education} graduated and currently working in {company} as {designation} Thank Youuu"

with DAG(
    dag_id = 'x_com_test',
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2)
    ) as dag:
    task1 = PythonOperator(
        task_id = 'get_name',
        python_callable = getname
    )

    task2 = PythonOperator(
        task_id = 'get_intro',
        python_callable  = getIntro
    )
    
    task3 = PythonOperator(
        task_id = 'info',
        python_callable = info
    )


    task1 >> task3 >> task2
