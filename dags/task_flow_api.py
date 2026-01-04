from airflow.decorators import dag,task
from datetime import datetime

@dag(dag_id = 'task_flow',schedule_interval = '@daily', start_date = datetime(2025,11,23))
def hello():
    @task
    def getname():
        return 'Suresh'
    @task
    def details():
        return {
            'age':'24',
            'college':'poornima university',
            'company':'tredence',
            'role':'data engineer'
        }

    @task
    def intro(name,dict1):

        print(f"my name is {name}, age is {dict1['age']} and company is {dict1['company']}")

    intro(getname(),details())

hello = hello()