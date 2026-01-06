from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

project_id = "smooth-ripple-392518"
params = {}

def list_gcs_objects():
    hook = GCSHook()
    files = hook.list(
        bucket_name="bkt_test_suresh_1",
        prefix="input/"
    )

    
    all_files = []
    for i in range(1,len(files)):
        file_name = files[i]
        all_files.append(file_name)

    print(all_files)

    bq_hook = BigQueryHook(location = "asia-south1",project_id="smooth-ripple-392518") 
    query = f"select file_name from dev_suresh.file_logs"

    loaded_file = bq_hook.get_records(query,parameters = params)

    loaded_files_new = []
    for i in loaded_file:
        loaded_files_new.append(i[0])
    print(loaded_files_new)

    new_files = [i for i in all_files if i not in loaded_file]
    return new_files


def write_audit_to_bq(files):
    # creating rows as key value pair
    rows = [{"file_name":f,"load_time":datetime.now()} for f in files]
    print(rows)

    bq_hook = BigQueryHook()
    bq_hook.insert_all(
        project_id="smooth-ripple-392518",
        dataset_id="dev_suresh",
        table_id="file_logs",
        rows=rows
    )


dag =  DAG(
    dag_id="composer_list_gcs_objects_new1",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)

list_files = PythonOperator(
    task_id="list_files",
    python_callable=list_gcs_objects,
    dag=dag,
)

audit = PythonOperator(
    task_id = 'audit',
    python_callable = write_audit_to_bq,
    # paramteres sending which is coming from task list_files
    op_args = [list_files.output],
    dag=dag
)


load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example",
    bucket="bkt_test_suresh_1",
    # directly fetching the files from function call, because jina syntax was not working
    source_objects=list_files.output,
    destination_project_dataset_table="smooth-ripple-392518.dev_suresh.tbl_names",
    write_disposition="WRITE_TRUNCATE",
    source_format="CSV",
    skip_leading_rows=1,
    dag=dag,
    autodetect=True
)


list_files >> load_csv >> audit
