from contextlib import aclosing
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from src.staging import StagingRepository
from src.staging.codec import serialize_batch
from src.staging.registry import SOURCES

from tests.integration.transformers._transformer_data import (
    make_bridge_batch,
    make_event_batch,
)

from ._data import CONTEXT, insert_manifest, make_dataset, missing_counts, read_dataset


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("source", SOURCES)
async def test_staging_repository_positive_records(client, source):
    """[integration] Raw staging round-trips every source, fields, repeated IDs and batch order."""
    repository = StagingRepository(client)
    dataset = make_dataset(source)
    batch = (
        make_event_batch()[0]
        if source == "fct_events"
        else make_bridge_batch(source.removeprefix("brg_"))[0]
    )
    if source == "fct_events":
        dataset.first_offset = 10
    await repository.write_batch(
        dataset, batch[:1], next_offset=12 if source == "fct_events" else None
    )
    await repository.write_batch(
        dataset, batch[1:], next_offset=19 if source == "fct_events" else None
    )
    await repository.write_missing(dataset, {"track": {UUID(int=1), UUID(int=2)}})
    await repository.write_missing(dataset, {"track": {UUID(int=1)}})
    reference = await repository.complete(dataset)
    loaded = await read_dataset(client, reference)
    assert loaded.dimension_counts == missing_counts(track=2)
    actual = []
    offsets = []
    async with aclosing(repository.read_batches(loaded)) as batches:
        async for rows, next_offset in batches:
            actual.extend(rows)
            offsets.append(next_offset)
    assert serialize_batch(source, actual) == serialize_batch(source, batch)
    assert offsets == ([12, 19] if source == "fct_events" else [None, None])
    assert loaded.data_expires_at == dataset.data_expires_at
    assert loaded.batch_count == 2
    assert loaded.row_count == len(batch)
    assert set(reference) == {"dataset_id"}


@pytest.mark.asyncio(loop_scope="session")
async def test_staging_repository_positive_empty_dataset(client):
    """[integration] A completed empty dataset is valid and has no expiring data."""
    repository = StagingRepository(client)
    reference = await repository.complete(make_dataset())
    loaded = await read_dataset(client, reference)
    assert loaded.batch_count == loaded.row_count == 0
    assert loaded.data_expires_at is None
    assert [batch async for batch in repository.read_batches(loaded)] == []


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("table", ["stg_batches_all", "stg_missing_dimension_ids_all"])
async def test_staging_repository_negative_incomplete_dataset(client, table):
    """[integration] Missing payloads or UUIDs cannot be mistaken for an empty dataset."""
    repository = StagingRepository(client)
    dataset = make_dataset()
    await repository.write_batch(dataset, make_bridge_batch("track_genre")[0])
    await repository.write_missing(dataset, {"track": {UUID(int=1)}})
    reference = await repository.complete(dataset)
    await client.command(f"TRUNCATE TABLE music_dwh.{table}")
    with pytest.raises(RuntimeError):
        await read_dataset(client, reference)


@pytest.mark.asyncio(loop_scope="session")
async def test_staging_repository_negative_unfinished_dataset(client):
    """[integration] Partial staging data without a manifest cannot be consumed."""
    repository = StagingRepository(client)
    dataset = make_dataset()
    await repository.write_batch(dataset, make_bridge_batch("track_genre")[0])
    with pytest.raises(LookupError):
        await read_dataset(client, dataset.reference())


@pytest.mark.asyncio(loop_scope="session")
async def test_staging_repository_negative_wrong_run(client):
    """[integration] A dataset is bound to its run, source and exact timestamp interval."""
    dataset = make_dataset()
    dataset.interval_start = dataset.interval_start.replace(microsecond=123456)
    context = {**CONTEXT, "timestamp_start": dataset.interval_start}
    reference = await StagingRepository(client).complete(dataset)
    assert (
        await StagingRepository(client).get(reference["dataset_id"], **context)
    ).interval_start == dataset.interval_start
    for overrides in (
        {"run_id": "another"},
        {"dag_id": "another"},
        {"timestamp_end": dataset.interval_end + timedelta(seconds=1)},
        {"source": "fct_events"},
    ):
        with pytest.raises(ValueError):
            await StagingRepository(client).get(
                reference["dataset_id"], **{**context, **overrides}
            )


@pytest.mark.asyncio(loop_scope="session")
async def test_staging_repository_positive_ttl(client):
    """[integration] Three-day TTL removes raw data and missing IDs but retains the manifest."""
    dataset = make_dataset()
    batch = make_bridge_batch("track_genre")[0]
    old = datetime.now(UTC).replace(microsecond=0) - timedelta(days=4)
    dataset.batch_count = 1
    dataset.row_count = len(batch)
    dataset.dimension_counts = missing_counts(track=1)
    dataset.data_expires_at = old + timedelta(days=3)
    await client.insert(
        "music_dwh.stg_batches_all",
        [
            (
                dataset.dataset_id,
                0,
                serialize_batch(dataset.source, batch),
                len(batch),
                None,
                old,
            )
        ],
        column_names=(
            "dataset_id",
            "batch_number",
            "payload",
            "row_count",
            "next_offset",
            "stored_at",
        ),
    )
    await client.insert(
        "music_dwh.stg_missing_dimension_ids_all",
        [(dataset.dataset_id, "track", UUID(int=1), old)],
        column_names=("dataset_id", "dimension", "entity_id", "stored_at"),
    )
    await insert_manifest(client, dataset)
    with pytest.raises(RuntimeError):
        await read_dataset(client, dataset.reference())
    for name in ("stg_batches_all", "stg_missing_dimension_ids_all"):
        await client.command(
            f"ALTER TABLE music_dwh.{name} MATERIALIZE TTL SETTINGS mutations_sync=1"
        )
        result = await client.query(f"SELECT count() FROM music_dwh.{name}")
        assert result.result_rows[0][0] == 0
    result = await client.query("SELECT count() FROM music_dwh.stg_datasets_all")
    assert result.result_rows[0][0] == 1
    query = (
        await client.query("SHOW CREATE TABLE music_dwh.stg_datasets_all")
    ).result_rows[0][0]
    assert "TTL" not in query
