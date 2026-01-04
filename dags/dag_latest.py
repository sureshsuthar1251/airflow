from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  # only if you need it

with DAG(
    dag_id="sql_dag_ex1",
    schedule="@daily",
    start_date=datetime(2025, 12, 2),
    catchup=False,
) as dag:

    create_table_mssql_task = SQLExecuteQueryOperator(
        task_id="create_country_table",
        conn_id="mssql",
        sql=r"""
        CREATE TABLE guest.country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
    )
