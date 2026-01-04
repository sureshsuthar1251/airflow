from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from sqlalchemy import create_engine

default_args = {
    'owner':'admin',
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }



def extractDataFromCovidAPI():
    import requests
    import pandas as pd
    url = "https://disease.sh/v3/covid-19/countries"
    data = requests.get(url)

    df = pd.DataFrame(data.json())
    df.to_csv('csv_covid.csv')

def readLoad():
    import pandas as pd
    df = pd.read_csv('/opt/airflow/csv_covid.csv')
    print(df.head())

    hook = MsSqlHook(mssql_conn_id="mssql")
    engine = hook.get_sqlalchemy_engine()
    table_name = 'covid_data'
    df.to_sql(
            table_name,
            con=engine,
            if_exists='replace', # Creates or replaces the table
            index=False,
            chunksize=1000 # Adjust as needed
        )
    print(f" data has been written to the databse and size is {len(df)} ")


with DAG(
    dag_id = 'api-project',
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 12, 9),
) as dag:
    task1 = PythonOperator(
        task_id = 'task1',
        python_callable = extractDataFromCovidAPI
    )
    task2 = PythonOperator(
        task_id = 'task2',
        python_callable = readLoad
    )
    task1 >> task2