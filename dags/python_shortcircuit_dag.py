from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

def check_if_even():
    number = 7  # Change this to test (even → True, odd → False)

    return number % 2 == 0  # True → continue, False → skip

def run_if_even():
    print("Number was even → this task runs")

def run_always():
    print("This task always runs at end")

with DAG(
    dag_id="short_circuit_example",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    check = ShortCircuitOperator(
        task_id="check_even",
        python_callable=check_if_even,
    )

    even_task = PythonOperator(
        task_id="run_even_task",
        python_callable=run_if_even,
    )

    final_task = PythonOperator(
        task_id="final_task",
        python_callable=run_always,
        trigger_rule="none_failed_min_one_success",
    )

    check >> even_task >> final_task
