from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
}

def upstream_task():
    dog_owner_data = {
        "names": ["Trevor", "Grant", "Marcy", "Carly", "Philip"],
        "dogs": [1, 2, 2, 0, 4],
    }
    return dog_owner_data

def prepare_bash_command(ti):
    dog_owner_data = ti.xcom_pull(task_ids="upstream_task")
    names_of_dogless_people = []

    for name, dog in zip(dog_owner_data["names"], dog_owner_data["dogs"]):
        if dog < 1:
            names_of_dogless_people.append(name)

    if names_of_dogless_people:
        if len(names_of_dogless_people) == 1:
            cmd = f'echo "{names_of_dogless_people[0]} urgently needs a dog!"'
        else:
            names = " and ".join(names_of_dogless_people)
            cmd = f'echo "{names} urgently need a dog!"'
    else:
        cmd = 'echo "All good, everyone has at least one dog!"'

    return cmd

with DAG(
    dag_id="dog_owner_dag_classic",
    default_args=default_args,
    start_date=datetime(2025, 11, 23),
    schedule_interval=None,
    catchup=False,
) as dag:

    upstream = PythonOperator(
        task_id="upstream_task",
        python_callable=upstream_task,
    )

    prepare_bash = PythonOperator(
        task_id="prepare_bash_task",
        python_callable=prepare_bash_command,
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="{{ ti.xcom_pull(task_ids='prepare_bash_task') }}",
    )

    upstream >> prepare_bash >> bash_task
