import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_mssql_new"


with DAG(
    DAG_ID,
    schedule_interval='@daily',
    start_date=datetime(2021, 10, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # Example of creating a task to create a table in MsSql

    create_table_mssql_task = MsSqlOperator(
        task_id='create_country_table',
        mssql_conn_id='mssql',
        sql=r"""
        CREATE TABLE Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
        dag=dag,
    )

    @dag.task(task_id="insert_mssql_task")
    def insert_mssql_hook():
        mssql_hook = MsSqlHook(mssql_conn_id='mssql', schema='airflow')

        rows = [
            ('India', 'Asia'),
            ('Germany', 'fsfsf'),
            ('Argentina', 'South America'),
            ('Ghana', 'Africa'),
            ('Japan', 'Asia'),
            ('Namibia', 'Africa'),
        ]
        target_fields = ['name', 'continent']
        mssql_hook.insert_rows(table='Country', rows=rows, target_fields=target_fields)
    # Example of creating a task that calls an sql command from an external file.
    create_table_mssql_from_external_file = MsSqlOperator(
        task_id='create_table_from_external_file',
        mssql_conn_id='mssql',
        sql='create_table.sql',
        dag=dag,
    )
    populate_user_table = MsSqlOperator(
        task_id='populate_user_table',
        mssql_conn_id='mssql',
        sql=r"""
                INSERT INTO Users (username, description)
                VALUES ( 'Danny', 'Musician');
                INSERT INTO Users (username, description)
                VALUES ( 'Simone', 'Chef');
                INSERT INTO Users (username, description)
                VALUES ( 'Lily', 'Florist');
                INSERT INTO Users (username, description)
                VALUES ( 'Tim', 'Pet shop owner');
                """,
    )
    get_all_countries = MsSqlOperator(
        task_id="get_all_countries",
        mssql_conn_id='mssql',
        sql=r"""SELECT * FROM Country;""",
    )
    get_all_description = MsSqlOperator(
        task_id="get_all_description",
        mssql_conn_id='mssql',
        sql=r"""SELECT description FROM Users;""",
    )
    get_countries_from_continent = MsSqlOperator(
        task_id="get_countries_from_continent",
        mssql_conn_id='mssql',
        sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
        params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
    )
    (
        create_table_mssql_task
        >> insert_mssql_hook()
        >> create_table_mssql_from_external_file
        >> populate_user_table
        >> get_all_countries
        >> get_all_description
        >> get_countries_from_continent
    )