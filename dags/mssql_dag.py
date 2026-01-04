import os
from datetime import datetime



from airflow import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_mssql_demo"


with DAG(
    DAG_ID,
    schedule="@daily",
    start_date=datetime(2025, 12, 9),
    tags=["example"],
    catchup=False,
) as dag:

    create_table_mssql_task = SQLExecuteQueryOperator(
        task_id='create_country_table',
        conn_id='mssql',
        sql=r"""
        CREATE TABLE Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
        dag=dag,
    )