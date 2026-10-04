from airflow import DAG
from airflow.sdk import task, get_current_context, Variable
from datetime import datetime
import pendulum

# [timestamp_start, timestamp_end)
first_date = pendulum.datetime(2026, 9, 28, 11, 31, tz="Europe/Moscow")

dag = DAG('etl_msf',
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 28, 11, 50, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1)

with dag:
    @task(task_id="fix_timestamp")
    def timestamp_fixing(ds=None, **kwargs):
        context = get_current_context()

        return {
            "timestamp_start": datetime.fromisoformat(Variable.get("etl_msf_last_timestamp_end", default=first_date.isoformat())),
            "timestamp_end": context["data_interval_start"].in_timezone("Europe/Moscow")
        }

    @task(task_id="initialize etl_load")
    def load_devices(interval):
        pass

    @task(task_id="swap_timestamps")
    def swap_timestamp(interval):
        Variable.set("etl_msf_last_timestamp_end", interval["timestamp_end"].isoformat())

    task_timestamp_fixing = timestamp_fixing()
    task_load_devices = load_devices(task_timestamp_fixing)
    task_swap_timestamp = swap_timestamp(task_timestamp_fixing)

    