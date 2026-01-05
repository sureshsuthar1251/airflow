from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def files_inside_gcs_bucket():
  hook = GCSHook()
  files = hook.list(
        bucket_name="bkt_test_suresh_1",
        prefix="input/"
    )
  print(files)
  


with DAG(DAG_ID = 'gcp_dummy_project', schedule_interval = '@daily',start_date = datetime(2026,01,05),    ) as dag:
  list_gcs_objects = PythonOperator(
    task_id = 'list_gcs_objects',
    python_callable = 'files_inside_gcs_bucket'
  )
  
