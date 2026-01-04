from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner' : 'admin',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG (
    dag_id = 'catch-up_backfill_dag',
    default_args = default_args,
    start_date = datetime(2025,11,20),
    schedule_interval = '0 4 L * *',
    # catchup is used to ensure that we want to run the dag for back dates or not. if yet them set it True else False.
    catchup = True
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo this is simple backfill dag'
    )