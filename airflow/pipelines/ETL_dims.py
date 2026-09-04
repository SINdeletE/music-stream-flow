from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta

dag = DAG('ETL_dims',
    schedule="@daily",
    start_date=datetime(2028, 12, 17, 0))

with dag:
    @task(task_id="helloworld")
    def helloworld(ds=None, **kwargs):
        print('hello, world')

    @task(task_id="worldhello")
    def worldhello(ds=None, **kwargs):
        print('world, hello')

    task_helloworld = helloworld()
    task_worldhello = worldhello()
    
    task_helloworld >> task_worldhello