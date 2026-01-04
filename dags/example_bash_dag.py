from airflow  import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id = 'example_bash_dag',start_date = datetime(2025,11,23), schedule_interval = '@daily') as dag:
    task1 = BashOperator(
        task_id = 'simple_bash_command',
        bash_command = "echo hello airflow"
    )
