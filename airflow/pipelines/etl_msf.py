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
    FctEventsPipeline,
)
from src.settings import KafkaSettings
from src.staging import (
    BrgArtistReleaseStaging,
    BrgDeviceAppReleaseStaging,
    BrgTrackArtistStaging,
    BrgTrackGenreStaging,
    BrgTrackReleaseStaging,
    CollectStaging,
    FctEventsStaging,
)

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
    (
        "load_brg_device_app_release",
        BrgDeviceAppReleasePipeline,
        BrgDeviceAppReleaseStaging,
    ),
    ("load_brg_track_genre", BrgTrackGenrePipeline, BrgTrackGenreStaging),
    ("load_brg_track_artist", BrgTrackArtistPipeline, BrgTrackArtistStaging),
    ("load_brg_track_release", BrgTrackReleasePipeline, BrgTrackReleaseStaging),
    ("load_brg_artist_release", BrgArtistReleasePipeline, BrgArtistReleaseStaging),
)


kafka_settings = KafkaSettings()
KAFKA_EVENTS_TASK_IDS = tuple(
    (f"load_kafka_events_partition_{i}", i)
    for i in range(kafka_settings.music_kafka_num_partitions)
)


def create_pipeline_task(
    task_id, pipeline, *, partition=None, ref_argument=None, return_ref=False
):
    @task(task_id=task_id)
    async def load(interval, dataset_ref=None):
        context = get_current_context()
        kwargs = {
            "timestamp_start": interval["timestamp_start"],
            "timestamp_end": interval["timestamp_end"],
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "task_id": context["task"].task_id,
        }
        if partition is not None:
            kwargs["partition"] = partition
        if ref_argument is not None:
            kwargs[ref_argument] = dataset_ref["dataset_id"]
        result = await pipeline.run(**kwargs)
        if return_ref:
            return result

    return load


# [timestamp_start, timestamp_end)
first_date = pendulum.datetime(2026, 10, 17, 11, 30, tz="UTC")

dag = DAG(
    "etl_msf",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 18, 9, 00, tz="UTC"),
    catchup=False,
    max_active_runs=1,
)

with dag:

    @task(task_id="fix_timestamp")
    def timestamp_fixing(ds=None, **kwargs):
        context = get_current_context()

        return {
            "timestamp_start": pendulum.parse(
                Variable.get(
                    "etl_msf_last_timestamp_end",
                    default=first_date.isoformat(),
                )
            ).in_timezone("UTC"),
            "timestamp_end": context["data_interval_start"].in_timezone("UTC"),
        }

    @task(task_id="swap_timestamp")
    def swap_timestamp(interval):
        Variable.set(
            "etl_msf_last_timestamp_end", interval["timestamp_end"].isoformat()
        )

    @task(task_id="collect")
    async def collect(refs, interval):
        context = get_current_context()
        return await CollectStaging.run(
            refs,
            timestamp_start=interval["timestamp_start"],
            timestamp_end=interval["timestamp_end"],
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            task_id=context["task"].task_id,
        )

    task_timestamp_fixing = timestamp_fixing()
    staged_brgs = [
        create_pipeline_task(
            task_id.replace("load_", "stage_", 1),
            staging,
            return_ref=True,
        )(task_timestamp_fixing)
        for task_id, _, staging in BRG_PIPELINES
    ]
    staged_events = [
        create_pipeline_task(
            task_id.replace("load_", "stage_", 1),
            FctEventsStaging,
            partition=partition,
            return_ref=True,
        )(task_timestamp_fixing)
        for task_id, partition in KAFKA_EVENTS_TASK_IDS
    ]
    task_collect = collect(staged_brgs + staged_events, task_timestamp_fixing)
    task_load_dims = [
        create_pipeline_task(
            task_id,
            pipeline,
            ref_argument="missing_ids_dataset_id",
        )(task_timestamp_fixing, task_collect)
        for task_id, pipeline in DIM_PIPELINES
    ]
    task_load_brgs = [
        create_pipeline_task(
            task_id,
            pipeline,
            ref_argument="staging_dataset_id",
        )(task_timestamp_fixing, ref)
        for (task_id, pipeline, _), ref in zip(BRG_PIPELINES, staged_brgs, strict=True)
    ]
    task_load_fct = [
        create_pipeline_task(
            task_id,
            FctEventsPipeline,
            partition=partition,
            ref_argument="staging_dataset_id",
        )(task_timestamp_fixing, ref)
        for (task_id, partition), ref in zip(
            KAFKA_EVENTS_TASK_IDS, staged_events, strict=True
        )
    ]
    task_swap_timestamp = swap_timestamp(task_timestamp_fixing)

    cross_downstream(task_load_dims, task_load_brgs)
    cross_downstream(task_load_brgs, task_load_fct)
    task_load_fct >> task_swap_timestamp
