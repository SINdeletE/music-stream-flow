import asyncio
from contextlib import aclosing
from unittest.mock import AsyncMock

import pytest
from confluent_kafka import OFFSET_INVALID
from sqlalchemy import delete
from src.connections import KafkaConsumerConnector, OltpConnector
from src.models.dwh import FctEvent
from src.staging import (
    BrgArtistReleaseStaging,
    BrgDeviceAppReleaseStaging,
    BrgTrackArtistStaging,
    BrgTrackGenreStaging,
    BrgTrackReleaseStaging,
    CollectStaging,
    FctEventsStaging,
    StagingRepository,
)
from src.staging.codec import serialize_batch
from src.staging.registry import DIMENSIONS, SOURCES

from tests.integration.pipelines._pipeline_data import (
    PARTITION,
    clear_clickhouse,
    make_event_payloads,
    prepare_kafka,
    seed_event_dimensions,
)
from tests.integration.transformers._transformer_data import (
    ENTITY_IDS,
    make_bridge_batch,
    seed_dimensions,
)

from ._data import CONTEXT, make_dataset, missing_counts, read_dataset, seed_sources

STAGING = {
    "brg_device_app_release": BrgDeviceAppReleaseStaging,
    "brg_track_genre": BrgTrackGenreStaging,
    "brg_track_artist": BrgTrackArtistStaging,
    "brg_track_release": BrgTrackReleaseStaging,
    "brg_artist_release": BrgArtistReleaseStaging,
}


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("source", STAGING)
@pytest.mark.parametrize("empty", [False, True], ids=["records", "empty"])
async def test_oltp_staging_positive_records(
    client, dwh_settings, oltp_settings, source, empty
):
    """[integration] Every bridge staging stores real OLTP batches and only unresolved UUIDs."""
    await seed_sources(oltp_settings)
    spec = SOURCES[source]
    missing_name = spec.dimensions[0]
    await seed_dimensions(client, spec.dimensions, missing_entity_name=missing_name)
    settings = oltp_settings.model_copy(update={"music_oltp_staging_batch_size": 1})
    if empty:
        async with OltpConnector(settings) as connector, connector.session() as session:
            await session.execute(delete(spec.model))
            await session.commit()
    reference = await STAGING[source].run(
        **CONTEXT, task_id="stage", oltp_settings=settings, dwh_settings=dwh_settings
    )
    dataset = await read_dataset(client, reference)
    expected = (
        []
        if empty
        else list(reversed(make_bridge_batch(source.removeprefix("brg_"))[0][:2]))
    )
    actual = []
    repository = StagingRepository(client)
    async with aclosing(repository.read_batches(dataset)) as batches:
        async for batch, offset in batches:
            assert len(batch) == 1
            assert offset is None
            actual.extend(batch)
    assert serialize_batch(source, actual) == serialize_batch(source, expected)
    assert dataset.row_count == len(expected)
    assert dataset.batch_count == len(expected)
    missing = await repository.missing_ids(dataset)
    assert missing[missing_name] == (set() if empty else {ENTITY_IDS[missing_name][1]})
    assert all(not ids for name, ids in missing.items() if name != missing_name)
    assert oltp_settings.music_oltp_batch_size == 4


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("count", [0, 8, 9])
async def test_fct_events_staging_positive_records(
    client, dwh_settings, kafka_address, monkeypatch, count
):
    """[integration] Kafka staging persists bounded raw batches without committing any offset."""
    await seed_event_dimensions(client)
    await clear_clickhouse(client, FctEvent)
    payloads = make_event_payloads(count)
    settings = await prepare_kafka(kafka_address, payloads)
    settings.music_kafka_staging_batch_size = 3
    with monkeypatch.context() as patch:
        commit = AsyncMock(side_effect=AssertionError("Unexpected staging commit"))
        patch.setattr(KafkaConsumerConnector, "commit", commit)
        reference = await FctEventsStaging.run(
            **CONTEXT,
            task_id="stage_events",
            partition=PARTITION,
            kafka_settings=settings,
            dwh_settings=dwh_settings,
        )
        commit.assert_not_awaited()
    dataset = await read_dataset(client, reference)
    restored = []
    offsets = []
    async with aclosing(StagingRepository(client).read_batches(dataset)) as batches:
        async for batch, next_offset in batches:
            assert 0 < len(batch) <= 3
            restored.extend(batch)
            offsets.append(next_offset)
    import json

    assert json.loads(serialize_batch("fct_events", restored)) == payloads
    assert dataset.row_count == count
    assert (offsets[-1] if offsets else None) == (count if count else None)
    assert dataset.dimension_counts == missing_counts()
    assert settings.music_kafka_batch_size == 4
    async with KafkaConsumerConnector(settings, PARTITION) as consumer:
        committed = await asyncio.to_thread(
            KafkaConsumerConnector.get_current_offset, consumer, settings, PARTITION
        )
    assert committed == OFFSET_INVALID


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse", [False, True])
async def test_collect_staging_positive_records(client, dwh_settings, reverse):
    """[integration] Collect unions overlapping UUID sets separately for every dimension."""
    repository = StagingRepository(client)
    references = []
    expected = {name: set() for name in DIMENSIONS}
    for index, source in enumerate(SOURCES):
        dataset = make_dataset(source)
        missing = {
            name: {ENTITY_IDS[name][0], ENTITY_IDS[name][1]}
            for name in SOURCES[source].dimensions
        }
        for name, identifiers in missing.items():
            expected[name].update(identifiers)
        await repository.write_missing(dataset, missing)
        references.append(await repository.complete(dataset))
    if reverse:
        references.reverse()
    references.append(references[0])
    reference = await CollectStaging.run(
        references, **CONTEXT, task_id="collect", dwh_settings=dwh_settings
    )
    dataset = await read_dataset(client, reference)
    assert dataset.source == "collect"
    assert await repository.missing_ids(dataset) == expected
    assert dataset.dimension_counts == {
        name: len(identifiers) for name, identifiers in expected.items()
    }
    assert dataset.batch_count == dataset.row_count == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_collect_staging_positive_empty_dataset(client, dwh_settings):
    """[integration] Collect publishes a valid empty result without skipping downstream tasks."""
    reference = await StagingRepository(client).complete(make_dataset())
    combined = await CollectStaging.run(
        [reference], **CONTEXT, task_id="collect", dwh_settings=dwh_settings
    )
    dataset = await read_dataset(client, combined)
    assert dataset.dimension_counts == missing_counts()
    assert dataset.data_expires_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_collect_staging_negative_wrong_run(client, dwh_settings):
    """[integration] Collect does not publish a result when an input belongs to another run."""
    dataset = make_dataset()
    dataset.run_id = "another_run"
    reference = await StagingRepository(client).complete(dataset)
    with pytest.raises(ValueError):
        await CollectStaging.run(
            [reference], **CONTEXT, task_id="collect", dwh_settings=dwh_settings
        )
    count = await client.query("SELECT count() FROM music_dwh.stg_datasets_all")
    assert count.result_rows[0][0] == 1
