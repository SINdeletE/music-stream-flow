from airflow import DAG
from airflow.sdk import task, get_current_context, Variable
from datetime import datetime
import dotenv

# [timestamp_start, timestamp_end)
first_date = datetime(2026, 9, 28, 11, 31)

dag = DAG('etl_msf',
    schedule="@hourly",
    start_date=datetime(2026, 9, 28, 11, 50),
    catchup=False,
    max_active_runs=1)

with dag:
    @task(task_id="fix_timestamp")
    def timestamp_fixing(ds=None, **kwargs):
        global timestamp_end
        
        context = get_current_context()

        return {
            "timestamp_start": Variable.get("etl_msf_last_timestamp_end", default=first_date),
            "timestamp_end": context["data_interval_start"]
        }

    @task(task_id="load_devices")
    def load_devices(interval):
        pass

    @task(task_id="swap_timestamps")
    def swap_timestamp(interval):
        Variable.set("etl_msf_last_timestamp_end", interval["timestamp_end"].isoformat())

    task_timestamp_fixing = timestamp_fixing()
    task_load_devices = load_devices(task_timestamp_fixing)
    task_swap_timestamp = swap_timestamp(task_timestamp_fixing)

    task_timestamp_fixing >> [task_load_devices, task_swap_timestamp]