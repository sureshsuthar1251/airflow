from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

def list_gcs_objects():
    hook = GCSHook()
    files = hook.list(
        bucket_name="my-bucket",
        prefix="input/"
    )
    print(files)

dag =  DAG(
    dag_id="composer_list_gcs_objects",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)

list_files = PythonOperator(
    task_id="list_files",
    python_callable=list_gcs_objects
)
