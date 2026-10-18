import asyncio
import json
from dataclasses import asdict
from unittest.mock import AsyncMock, Mock

import pytest
from confluent_kafka import OFFSET_INVALID
from sqlalchemy import delete
from src.connections import KafkaConsumerConnector, OltpConnector
from src.loaders import DimTrackLoader, FctEventLoader
from src.models.dwh import BrgTrackGenre, DimTrack, FctEvent
from src.models.oltp import TrackGenre
from src.pipelines import BrgTrackGenrePipeline, DimTrackPipeline, FctEventsPipeline
from src.staging import (
    BrgTrackGenreStaging,
    CollectStaging,
    FctEventsStaging,
    StagingRepository,
)
from src.staging.codec import serialize_batch
from src.staging.registry import DIMENSIONS

from tests.integration.pipelines._pipeline_data import (
    PARTITION,
    assert_loaded_rows,
    clear_clickhouse,
    make_event_payloads,
    make_tracks,
    prepare_kafka,
    seed_event_dimensions,
    seed_tracks,
)
from tests.integration.transformers._transformer_data import (
    ENTITY_IDS,
    make_dimension_batch,
    make_event_batch,
    seed_dimensions,
)

from ._data import CONTEXT, read_dataset, reset_postgresql, seed_sources


async def committed(settings):
    async with KafkaConsumerConnector(settings, PARTITION) as consumer:
        return await asyncio.to_thread(
            KafkaConsumerConnector.get_current_offset, consumer, settings, PARTITION
        )


async def seed_cold_dimensions(client):
    await seed_dimensions(client, (*DIMENSIONS, "event_type"))
    await client.command("TRUNCATE TABLE music_dwh.dim_track_all")
    known = next(
        row
        for row in make_dimension_batch("track")[1]
        if row.track_id == ENTITY_IDS["track"][0]
    )
    known.track_sk = 99
    await DimTrackLoader(client).load_batch([known])


@pytest.mark.asyncio(loop_scope="session")
async def test_brg_track_genre_pipeline_positive_missing_dimension(
    client,
    dwh_settings,
    oltp_settings,
    monkeypatch,
):
    """[integration] Staged bridges resolve a cold dimension whose update is after the cutoff."""
    await clear_clickhouse(client, DimTrack, BrgTrackGenre)
    await seed_sources(oltp_settings, outside_dimensions=True)
    await seed_cold_dimensions(client)
    reference = await BrgTrackGenreStaging.run(
        **CONTEXT,
        task_id="stage",
        oltp_settings=oltp_settings,
        dwh_settings=dwh_settings,
    )
    combined = await CollectStaging.run(
        [reference], **CONTEXT, task_id="collect", dwh_settings=dwh_settings
    )
    dataset = await read_dataset(client, combined)
    assert (await StagingRepository(client).missing_ids(dataset))["track"] == {
        ENTITY_IDS["track"][1]
    }
    audit = await DimTrackPipeline.run(
        **CONTEXT,
        task_id="load_track",
        oltp_settings=oltp_settings,
        dwh_settings=dwh_settings,
        missing_ids_dataset_id=combined["dataset_id"],
    )
    assert audit.rows_read == audit.rows_written == 1
    rows = await client.query(
        "SELECT track_id, track_sk, updated_at FROM music_dwh.dim_track_all WHERE load_id = {load_id:UUID}",
        parameters={"load_id": audit.load_id},
        tz_mode="aware",
    )
    assert rows.result_rows[0][0:2] == (ENTITY_IDS["track"][1], 0)
    assert rows.result_rows[0][2] > CONTEXT["timestamp_end"]
    async with (
        OltpConnector(oltp_settings) as connection,
        connection.session() as session,
    ):
        await session.execute(delete(TrackGenre))
        await session.commit()
    guard = Mock(side_effect=AssertionError("Unexpected OLTP reread"))
    monkeypatch.setattr("src.pipelines._pipeline.OltpConnector", guard)
    audit = await BrgTrackGenrePipeline.run(
        **CONTEXT,
        task_id="load_bridge",
        dwh_settings=dwh_settings,
        staging_dataset_id=reference["dataset_id"],
    )
    guard.assert_not_called()
    assert audit.rows_read == audit.rows_written == 2
    result = await client.query(
        "SELECT track_sk, genre_sk FROM music_dwh.brg_track_genre_all WHERE load_id = {load_id:UUID}",
        parameters={"load_id": audit.load_id},
    )
    assert sorted(result.result_rows) == [(0, 0), (99, 61)]


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("count", [0, 8, 9])
async def test_brg_track_genre_pipeline_positive_staging_batch_sizes(
    client,
    dwh_settings,
    oltp_settings,
    count,
):
    """[integration] OLTP staging respects its own batch size for zero, full and partial batches."""
    await reset_postgresql(oltp_settings)
    await clear_clickhouse(client, DimTrack, BrgTrackGenre)
    tracks = make_tracks(count)
    genre = make_dimension_batch("genre")[0][0]
    links = [
        TrackGenre(track_id=row.id, genre_id=genre.id, created_at=row.updated_at)
        for row in tracks
    ]
    async with (
        OltpConnector(oltp_settings) as connection,
        connection.session() as session,
    ):
        session.add_all(tracks)
        session.add(genre)
        await session.flush()
        session.add_all(links)
        await session.commit()
    await seed_tracks(client, tracks)
    await seed_dimensions(client, ("genre",))
    settings = oltp_settings.model_copy(
        update={"music_oltp_staging_batch_size": 4, "music_oltp_batch_size": 11}
    )
    reference = await BrgTrackGenreStaging.run(
        **CONTEXT, task_id="stage", oltp_settings=settings, dwh_settings=dwh_settings
    )
    dataset = await read_dataset(client, reference)
    assert dataset.row_count == count
    assert dataset.batch_count == (count + 3) // 4
    result = await client.query(
        "SELECT batch_number, row_count FROM music_dwh.stg_batches_all WHERE dataset_id = {id:UUID} ORDER BY batch_number",
        parameters={"id": dataset.dataset_id},
    )
    assert [row[1] for row in result.result_rows] == [
        min(4, count - i) for i in range(0, count, 4)
    ]
    audit = await BrgTrackGenrePipeline.run(
        **CONTEXT,
        task_id="bridge",
        staging_dataset_id=dataset.dataset_id,
        dwh_settings=dwh_settings,
    )
    assert audit.rows_read == audit.rows_written == count


@pytest.mark.asyncio(loop_scope="session")
async def test_fct_events_pipeline_positive_missing_dimension(
    client,
    dwh_settings,
    oltp_settings,
    kafka_address,
):
    """[integration] Kafka staging plus collect repairs a missing track before facts are committed."""
    await clear_clickhouse(client, DimTrack, FctEvent)
    await seed_sources(oltp_settings, outside_dimensions=True)
    await seed_cold_dimensions(client)
    events, expected = make_event_batch()
    settings = await prepare_kafka(
        kafka_address, json.loads(serialize_batch("fct_events", events))
    )
    settings.music_kafka_staging_batch_size = 2
    reference = await FctEventsStaging.run(
        **CONTEXT,
        task_id="stage_events",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
    )
    assert await committed(settings) == OFFSET_INVALID
    combined = await CollectStaging.run(
        [reference], **CONTEXT, task_id="collect", dwh_settings=dwh_settings
    )
    audit = await DimTrackPipeline.run(
        **CONTEXT,
        task_id="track",
        oltp_settings=oltp_settings,
        dwh_settings=dwh_settings,
        missing_ids_dataset_id=combined["dataset_id"],
    )
    assert audit.rows_written == 1
    audit = await FctEventsPipeline.run(
        **CONTEXT,
        task_id="events",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
        staging_dataset_id=reference["dataset_id"],
    )
    rows = []
    for event, row in zip(events, expected, strict=True):
        values = asdict(row)
        values.update(event_sk=0, load_id=audit.load_id)
        if event.track_id is not None:
            values["track_sk"] = 99 if event.track_id == ENTITY_IDS["track"][0] else 0
        rows.append(values)
    await assert_loaded_rows(client, FctEvent, rows, audit.load_id, key="event_id")
    assert audit.rows_written == len(events)
    assert await committed(settings) == len(events)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("count", [0, 8, 9])
async def test_fct_events_pipeline_positive_staged_records(
    client,
    dwh_settings,
    kafka_address,
    monkeypatch,
    count,
):
    """[integration] Facts use frozen Kafka batches and retry skips already committed batch boundaries."""
    await clear_clickhouse(client, FctEvent)
    await seed_event_dimensions(client)
    settings = await prepare_kafka(kafka_address, make_event_payloads(count))
    settings.music_kafka_staging_batch_size = 4
    reference = await FctEventsStaging.run(
        **CONTEXT,
        task_id="stage",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
    )
    dataset = await read_dataset(client, reference)
    query = await client.query(
        "SELECT next_offset FROM music_dwh.stg_batches_all WHERE dataset_id = {id:UUID} ORDER BY batch_number LIMIT 1",
        parameters={"id": dataset.dataset_id},
    )
    first_offset = query.result_rows[0][0] if count else 0
    if count:
        async with KafkaConsumerConnector(settings, PARTITION) as consumer:
            await KafkaConsumerConnector.commit(
                consumer, settings, PARTITION, next_offset=first_offset
            )
    guard = Mock(side_effect=AssertionError("Unexpected Kafka reread"))
    monkeypatch.setattr(
        "src.pipelines.fct_events_pipeline.KafkaAnalyticsEventExtractor", guard
    )
    audit = await FctEventsPipeline.run(
        **CONTEXT,
        task_id="load",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
        staging_dataset_id=reference["dataset_id"],
    )
    guard.assert_not_called()
    assert audit.rows_read == audit.rows_written == count - first_offset
    assert await committed(settings) == (count if count else OFFSET_INVALID)
    again = await FctEventsPipeline.run(
        **CONTEXT,
        task_id="retry",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
        staging_dataset_id=reference["dataset_id"],
    )
    assert again.rows_read == again.rows_written == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_fct_events_pipeline_negative_staged_insert_failure(
    client,
    dwh_settings,
    kafka_address,
    monkeypatch,
):
    """[integration] Failed fact insertion leaves offsets unchanged and allows replay of frozen data."""
    await seed_event_dimensions(client)
    settings = await prepare_kafka(kafka_address, make_event_payloads(9))
    settings.music_kafka_staging_batch_size = 4
    reference = await FctEventsStaging.run(
        **CONTEXT,
        task_id="stage",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
    )
    with monkeypatch.context() as patch:
        load = AsyncMock(side_effect=RuntimeError("Insert failed"))
        patch.setattr(FctEventLoader, "load_batch", load)
        with pytest.raises(RuntimeError):
            await FctEventsPipeline.run(
                **CONTEXT,
                task_id="load",
                partition=PARTITION,
                kafka_settings=settings,
                dwh_settings=dwh_settings,
                staging_dataset_id=reference["dataset_id"],
            )
        assert load.await_count == 1
    assert await committed(settings) == OFFSET_INVALID


@pytest.mark.asyncio(loop_scope="session")
async def test_fct_events_pipeline_negative_offset_inside_staged_batch(
    client,
    dwh_settings,
    kafka_address,
):
    """[integration] A committed offset inside a frozen batch fails without further commits."""
    await seed_event_dimensions(client)
    settings = await prepare_kafka(kafka_address, make_event_payloads(9))
    settings.music_kafka_staging_batch_size = 9
    reference = await FctEventsStaging.run(
        **CONTEXT,
        task_id="stage",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
    )
    async with KafkaConsumerConnector(settings, PARTITION) as consumer:
        await KafkaConsumerConnector.commit(
            consumer, settings, PARTITION, next_offset=1
        )
    with pytest.raises(RuntimeError):
        await FctEventsPipeline.run(
            **CONTEXT,
            task_id="load",
            partition=PARTITION,
            kafka_settings=settings,
            dwh_settings=dwh_settings,
            staging_dataset_id=reference["dataset_id"],
        )
    assert await committed(settings) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_fct_events_pipeline_positive_earliest_reset(
    client,
    dwh_settings,
    kafka_address,
):
    """[integration] An unavailable committed offset resets to earliest without losing staged data."""
    await seed_event_dimensions(client)
    settings = await prepare_kafka(kafka_address, make_event_payloads(3))
    async with KafkaConsumerConnector(settings, PARTITION) as consumer:
        await KafkaConsumerConnector.commit(
            consumer, settings, PARTITION, next_offset=100
        )
    reference = await FctEventsStaging.run(
        **CONTEXT,
        task_id="stage",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
    )
    dataset = await read_dataset(client, reference)
    assert dataset.committed_offset == 100
    assert dataset.first_offset == 0
    assert dataset.next_offset == 3
    assert await committed(settings) == 100
    audit = await FctEventsPipeline.run(
        **CONTEXT,
        task_id="load",
        partition=PARTITION,
        kafka_settings=settings,
        dwh_settings=dwh_settings,
        staging_dataset_id=reference["dataset_id"],
    )
    assert audit.rows_read == audit.rows_written == 3
    assert await committed(settings) == 3


@pytest.mark.asyncio(loop_scope="session")
async def test_dim_track_pipeline_positive_empty_collect(
    client,
    dwh_settings,
    oltp_settings,
):
    """[integration] An empty collect does not disable the normal incremental dimension scan."""
    await seed_sources(oltp_settings)
    await seed_dimensions(client, ("track",))
    combined = await CollectStaging.run(
        [], **CONTEXT, task_id="collect", dwh_settings=dwh_settings
    )
    audit = await DimTrackPipeline.run(
        **CONTEXT,
        task_id="track",
        oltp_settings=oltp_settings,
        dwh_settings=dwh_settings,
        missing_ids_dataset_id=combined["dataset_id"],
    )
    assert audit.rows_read == audit.rows_written == 2
