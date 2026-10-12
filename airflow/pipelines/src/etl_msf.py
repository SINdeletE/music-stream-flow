from datetime import datetime

import pendulum
from airflow.sdk import DAG, Variable, cross_downstream, get_current_context, task

from src.pipelines import (
    BrgArtistReleasePipeline,
    BrgDeviceAppReleasePipeline,
    BrgTrackArtistPipeline,
    BrgTrackGenrePipeline,
    BrgTrackReleasePipeline,
    DimAppReleasePipeline,
    DimArtistPipeline,
    DimDeviceOsPipeline,
    DimDevicePipeline,
    DimGenrePipeline,
    DimReleasePipeline,
    DimTrackPipeline,
)
from src.settings import KafkaSettings

DIM_PIPELINES = (
    ("load_device", DimDevicePipeline),
    ("load_device_os", DimDeviceOsPipeline),
    ("load_app_release", DimAppReleasePipeline),
    ("load_track", DimTrackPipeline),
    ("load_artist", DimArtistPipeline),
    ("load_genre", DimGenrePipeline),
    ("load_release", DimReleasePipeline),
)

BRG_PIPELINES = (
    ("load_brg_device_app_release", BrgDeviceAppReleasePipeline),
    ("load_brg_track_genre", BrgTrackGenrePipeline),
    ("load_brg_track_artist", BrgTrackArtistPipeline),
    ("load_brg_track_release", BrgTrackReleasePipeline),
    ("load_brg_artist_release", BrgArtistReleasePipeline),
)


kafka_settings = KafkaSettings()
KAFKA_EVENTS_TASK_IDS = (
    (f"load_kafka_events_partition_{i!s}", i)
    for i in range(1, kafka_settings.music_kafka_num_partitions + 1)
)


def create_load_task(task_id, pipeline):
    @task(task_id=task_id)
    async def load(interval):
        context = get_current_context()

        await pipeline.run(
            timestamp_start=interval["timestamp_start"],
            timestamp_end=interval["timestamp_end"],
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            task_id=context["task"].task_id,
        )

    return load


# def create_load_kafka_events_task(task_id, num_partition):
#     @task(task_id=task_id)
#     async def load(interval):
#         context = get_current_context()

#         await

# [timestamp_start, timestamp_end)
first_date = pendulum.datetime(2026, 9, 28, 11, 31, tz="Europe/Moscow")

dag = DAG(
    "etl_msf",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 28, 11, 50, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
)

with dag:

    @task(task_id="fix_timestamp")
    def timestamp_fixing(ds=None, **kwargs):
        context = get_current_context()

        return {
            "timestamp_start": datetime.fromisoformat(
                Variable.get(
                    "etl_msf_last_timestamp_end", default=first_date.isoformat()
                )
            ),
            "timestamp_end": context["data_interval_start"].in_timezone(
                "Europe/Moscow"
            ),
        }

    @task(task_id="swap_timestamp")
    def swap_timestamp(interval):
        Variable.set(
            "etl_msf_last_timestamp_end", interval["timestamp_end"].isoformat()
        )

    task_timestamp_fixing = timestamp_fixing()
    task_load_dims = [
        create_load_task(task_id, pipeline)(task_timestamp_fixing)
        for task_id, pipeline in DIM_PIPELINES
    ]
    task_load_brgs = [
        create_load_task(task_id, pipeline)(task_timestamp_fixing)
        for task_id, pipeline in BRG_PIPELINES
    ]
    task_swap_timestamp = swap_timestamp(task_timestamp_fixing)

    task_timestamp_fixing >> task_load_dims
    cross_downstream(task_load_dims, task_load_brgs)
    task_load_brgs >> task_swap_timestamp
