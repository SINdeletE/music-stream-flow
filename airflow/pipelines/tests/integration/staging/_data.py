from dataclasses import fields
from datetime import timedelta

from sqlalchemy import text
from src.connections import OltpConnector
from src.staging import Dataset, StagingRepository
from src.staging.registry import DIMENSIONS

from tests.integration.pipelines._pipeline_data import DAG_ID, END, RUN_ID, START
from tests.integration.transformers._transformer_data import (
    make_bridge_batch,
    make_dimension_batch,
)

CONTEXT = {
    "timestamp_start": START,
    "timestamp_end": END,
    "dag_id": DAG_ID,
    "run_id": RUN_ID,
}


def make_dataset(source="brg_track_genre"):
    return Dataset(
        source=source,
        dag_id=DAG_ID,
        run_id=RUN_ID,
        task_id="stage",
        interval_start=START,
        interval_end=END,
    )


async def read_dataset(client, reference, **kwargs):
    return await StagingRepository(client).get(
        reference["dataset_id"], **{**CONTEXT, **kwargs}
    )


async def reset_postgresql(settings):
    async with OltpConnector(settings) as connector, connector.session() as session:
        await session.execute(
            text(
                "TRUNCATE music_oltp.installation, music_oltp.track_genre, "
                "music_oltp.track_artist, music_oltp.track_release, music_oltp.artist_release, "
                "music_oltp.device_os_history, music_oltp.device, music_oltp.app_release, "
                "music_oltp.track, music_oltp.artist, music_oltp.genre, music_oltp.release"
            )
        )
        await session.commit()


async def seed_sources(settings, *, outside_dimensions=False):
    await reset_postgresql(settings)
    dimensions = {name: make_dimension_batch(name)[0] for name in DIMENSIONS}
    if outside_dimensions:
        for rows in dimensions.values():
            for row in rows:
                if hasattr(row, "updated_at"):
                    row.updated_at = END + timedelta(days=1)
    async with OltpConnector(settings) as connector, connector.session() as session:
        for name, rows in dimensions.items():
            if name != "device_os":
                session.add_all(rows)
        await session.flush()
        session.add_all(dimensions["device_os"])
        await session.flush()
        for name in (
            "device_app_release",
            "track_genre",
            "track_artist",
            "track_release",
            "artist_release",
        ):
            batch, _ = make_bridge_batch(name)
            session.add_all(batch[:2])
        await session.commit()
    return dimensions


async def insert_manifest(client, dataset):
    columns = tuple(item.name for item in fields(Dataset))
    await client.insert(
        "music_dwh.stg_datasets_all",
        [tuple(getattr(dataset, key) for key in columns)],
        column_names=columns,
    )


def missing_counts(**values):
    return {name: values.get(name, 0) for name in DIMENSIONS}
