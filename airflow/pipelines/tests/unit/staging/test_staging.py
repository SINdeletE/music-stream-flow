import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient
from pydantic import ValidationError
from sqlalchemy import inspect
from src.extractors import KafkaAnalyticsEventExtractor, TrackExtractor
from src.models.dwh import StgBatch, StgDataset, StgMissingDimensionId
from src.models.kafka import AnalyticsEvent
from src.models.kafka.analytics_event import EventType
from src.settings import KafkaSettings, OltpSettings
from src.staging import Dataset, StagingRepository
from src.staging.codec import deserialize_batch, serialize_batch
from src.staging.processes import find_missing
from src.staging.registry import SOURCES

NOW = datetime(2026, 10, 11, 12, 0, 0, 123456, tzinfo=UTC)


def make_batch(source):
    model = SOURCES[source].model
    if model is AnalyticsEvent:
        return [
            AnalyticsEvent(
                event_id=UUID(int=1),
                event_type=EventType.session_started,
                device_id=UUID(int=2),
                device_os_id=UUID(int=3),
                app_release_id=UUID(int=4),
                session_id=UUID(int=5),
                event_timestamp=NOW,
                received_at=NOW,
            )
        ]
    values = {}
    for column in inspect(model).columns:
        kind = column.type.python_type
        values[column.key] = (
            None
            if column.nullable
            else UUID(int=1)
            if kind is UUID
            else NOW
            if kind is datetime
            else True
            if kind is bool
            else 1
            if kind is int
            else "English value 🎵"
        )
    return [model(**values)]


@pytest.mark.parametrize("source", SOURCES)
def test_staging_codec_positive_records(source):
    """[unit] Staging preserves all source fields, UUIDs, timestamps and nullable values."""
    batch = make_batch(source)
    payload = serialize_batch(source, batch)
    restored = deserialize_batch(source, payload)
    assert serialize_batch(source, restored) == payload
    assert restored[0] is not batch[0]
    if source == "fct_events":
        assert asdict(restored[0]) == asdict(batch[0])
        assert json.loads(payload)[0]["event_type"] == "session_started"
        assert restored[0].track_id is None


@pytest.mark.parametrize("payload", ["{}", "[null]", '[{"unknown": 1}]'])
def test_staging_codec_negative_invalid_payload(payload):
    """[unit] Invalid staged data is rejected instead of constructing partial entities."""
    with pytest.raises((ValueError, TypeError)):
        deserialize_batch("brg_track_genre", payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("source", SOURCES)
async def test_staging_lookup_positive_records(source):
    """[unit] Missing UUID sets are unique, ignore null tracks, and preserve SK zero."""
    batch = make_batch(source)
    client = AsyncMock(spec=AsyncClient)
    client.query.return_value = SimpleNamespace(result_rows=[(UUID(int=1), 0)])
    result = await find_missing(client, source, batch + batch)
    for dimension in SOURCES[source].dimensions:
        identifier = getattr(batch[0], f"{dimension}_id")
        assert result[dimension] == (
            {identifier} if identifier not in (None, UUID(int=1)) else set()
        )
    assert all(
        call.kwargs["parameters"]["entity_ids"] for call in client.query.await_args_list
    )
    if source == "fct_events":
        assert result["track"] == set()
        assert client.query.await_count == 3


@pytest.mark.asyncio
async def test_staging_repository_negative_write_failure():
    """[unit] A failed batch insert does not advance counters or publish a dataset."""
    client = AsyncMock(spec=AsyncClient)
    client.insert.side_effect = RuntimeError("Insert failed")
    dataset = Dataset(
        source="brg_track_genre",
        dag_id="dag",
        run_id="run",
        task_id="stage",
        interval_start=NOW,
        interval_end=NOW,
    )
    with pytest.raises(RuntimeError):
        await StagingRepository(client).write_batch(dataset, make_batch(dataset.source))
    assert dataset.batch_count == dataset.row_count == 0
    assert dataset.next_offset is None
    assert client.insert.await_count == 1
    arguments = client.insert.await_args.kwargs
    assert arguments["table"] == StgBatch.table_to_insert()
    assert arguments["column_names"] == StgBatch.column_names()
    row = StgBatch(
        **dict(zip(arguments["column_names"], arguments["data"][0], strict=True))
    )
    assert row.dataset_id == dataset.dataset_id
    assert row.batch_number == 0
    assert row.row_count == 1
    assert row.next_offset is None
    assert row.stored_at.utcoffset() == timedelta(0)
    assert row.payload == serialize_batch(dataset.source, make_batch(dataset.source))


@pytest.mark.asyncio
async def test_staging_repository_positive_model_inserts():
    """[unit] Repository inserts modeled batches, UUIDs and the final manifest via loaders."""
    client = AsyncMock(spec=AsyncClient)
    client.query.return_value = SimpleNamespace(named_results=lambda: iter(()))
    repository = StagingRepository(client)
    dataset = Dataset(
        source="fct_events",
        dag_id="dag",
        run_id="run",
        task_id="stage",
        interval_start=NOW,
        interval_end=NOW,
    )
    assert repository.batch_loader.client is client
    assert repository.missing_id_loader.client is client
    assert repository.dataset_loader.client is client
    await repository.write_batch(dataset, [], next_offset=99)
    await repository.write_missing(dataset, {"track": set()})
    client.insert.assert_not_called()

    source_batch = make_batch(dataset.source)
    await repository.write_batch(dataset, source_batch, next_offset=0)
    assert dataset.batch_count == dataset.row_count == 1
    assert dataset.next_offset == 0
    await repository.write_missing(
        dataset, {"track": {UUID(int=2), UUID(int=1)}, "device": {UUID(int=3)}}
    )
    reference = await repository.complete(dataset)

    assert reference == dataset.reference()
    assert client.insert.await_count == 3
    for call, model in zip(
        client.insert.await_args_list,
        (StgBatch, StgMissingDimensionId, StgDataset),
        strict=True,
    ):
        assert call.kwargs["table"] == model.table_to_insert()
        assert call.kwargs["column_names"] == model.column_names()
    batch_call, missing_call, manifest_call = client.insert.await_args_list
    batch = StgBatch(
        **dict(zip(StgBatch.column_names(), batch_call.kwargs["data"][0], strict=True))
    )
    assert batch.payload == serialize_batch(dataset.source, source_batch)
    assert batch.next_offset == 0
    missing = [
        StgMissingDimensionId(
            **dict(zip(StgMissingDimensionId.column_names(), row, strict=True))
        )
        for row in missing_call.kwargs["data"]
    ]
    assert [(row.dimension, row.entity_id) for row in missing] == [
        ("track", UUID(int=1)),
        ("track", UUID(int=2)),
        ("device", UUID(int=3)),
    ]
    assert all(row.dataset_id == dataset.dataset_id for row in missing)
    assert manifest_call.kwargs["data"] == [dataset.to_row()]


@pytest.mark.asyncio
async def test_staging_repository_negative_expired_dataset():
    """[unit] Logical expiry is enforced before any ClickHouse query."""
    client = AsyncMock(spec=AsyncClient)
    dataset = Dataset(
        source="collect",
        dag_id="dag",
        run_id="run",
        task_id="collect",
        interval_start=NOW,
        interval_end=NOW,
        data_expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    with pytest.raises(RuntimeError):
        await StagingRepository(client).read_batches(dataset).__anext__()
    client.query.assert_not_called()


@pytest.mark.parametrize("size", [0, -1])
@pytest.mark.parametrize("kind", ["oltp", "kafka"])
def test_staging_settings_negative_batch_size(kind, size):
    """[unit] Non-positive staging batch sizes are rejected by settings."""
    if kind == "oltp":
        settings = OltpSettings
        kwargs = {
            "music_oltp_db_user": "test",
            "music_oltp_db_password": "password",
            "music_oltp_db_name": "test",
            "music_oltp_db_port": 5432,
            "music_oltp_batch_size": 4,
            "music_oltp_staging_batch_size": size,
        }
    else:
        settings = KafkaSettings
        kwargs = {
            "music_kafka_host": "test",
            "music_kafka_port": "9092",
            "music_kafka_topic_name": "events",
            "music_kafka_num_partitions": 3,
            "music_kafka_batch_size": 4,
            "music_kafka_timeout": 1,
            "music_kafka_staging_batch_size": size,
        }
    with pytest.raises(ValidationError):
        settings(_env_file=None, **kwargs)


@pytest.mark.parametrize(
    "extractor, field",
    [
        (TrackExtractor, "music_oltp_batch_size"),
        (KafkaAnalyticsEventExtractor, "music_kafka_batch_size"),
    ],
)
def test_staging_batch_size_positive_override(extractor, field):
    """[unit] A staging batch override does not change shared transport settings."""
    settings = SimpleNamespace(**{field: 11}, music_kafka_timeout=1)
    assert extractor(Mock(), settings, batch_size=3).batch_size == 3
    assert getattr(settings, field) == 11
    assert extractor(Mock(), settings).batch_size == 11
    with pytest.raises(ValueError):
        extractor(Mock(), settings, batch_size=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["brg", "fct", "collect"])
@pytest.mark.parametrize("case", ["start", "end", "both", "reversed"])
async def test_staging_negative_invalid_interval(monkeypatch, kind, case):
    """[unit] Invalid intervals are rejected before settings or connections are opened."""
    from src.staging import BrgTrackGenreStaging, CollectStaging, FctEventsStaging

    start, end = NOW, NOW + timedelta(hours=1)
    if case in ("start", "both"):
        start = start.replace(tzinfo=None)
    if case in ("end", "both"):
        end = end.replace(tzinfo=None)
    if case == "reversed":
        start, end = end, start
    guard = Mock(side_effect=AssertionError("Unexpected connection"))
    monkeypatch.setattr("src.staging.processes.DwhConnector", guard)
    kwargs = {
        "timestamp_start": start,
        "timestamp_end": end,
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "stage",
    }
    with pytest.raises(ValueError):
        if kind == "brg":
            await BrgTrackGenreStaging.run(**kwargs)
        elif kind == "fct":
            await FctEventsStaging.run(**kwargs, partition=1)
        else:
            await CollectStaging.run([], **kwargs)
    guard.assert_not_called()


@pytest.mark.asyncio
async def test_staging_negative_publish_after_write_error(monkeypatch):
    """[unit] Failed staging closes its resources and never publishes a complete manifest."""
    from contextlib import asynccontextmanager

    from src.staging import BrgTrackGenreStaging

    closed = []

    @asynccontextmanager
    async def dwh(settings):
        try:
            yield AsyncMock(spec=AsyncClient)
        finally:
            closed.append("dwh")

    @asynccontextmanager
    async def session():
        try:
            yield Mock()
        finally:
            closed.append("session")

    connector = Mock()
    connector.__aenter__ = AsyncMock(return_value=connector)
    connector.__aexit__ = AsyncMock(return_value=False)
    connector.session = session

    async def batches(*args, **kwargs):
        try:
            yield make_batch("brg_track_genre")
        finally:
            closed.append("batches")

    extractor = Mock()
    extractor.read_batches = batches
    from src.staging.registry import SOURCES, Source

    source = SOURCES["brg_track_genre"]
    monkeypatch.setitem(
        SOURCES,
        "brg_track_genre",
        Source(source.model, source.dimensions, Mock(return_value=extractor)),
    )
    monkeypatch.setattr("src.staging.processes.DwhConnector", dwh)
    monkeypatch.setattr(
        "src.staging.processes.OltpConnector", Mock(return_value=connector)
    )
    monkeypatch.setattr(
        "src.staging.processes.find_missing", AsyncMock(return_value={})
    )
    repository = Mock()
    repository.write_batch = AsyncMock(side_effect=RuntimeError("Write failed"))
    repository.complete = AsyncMock()
    monkeypatch.setattr(
        "src.staging.processes.StagingRepository", Mock(return_value=repository)
    )
    with pytest.raises(RuntimeError):
        await BrgTrackGenreStaging.run(
            timestamp_start=NOW,
            timestamp_end=NOW,
            dag_id="dag",
            run_id="run",
            task_id="stage",
            oltp_settings=SimpleNamespace(music_oltp_staging_batch_size=3),
        )
    repository.complete.assert_not_awaited()
    assert closed == ["batches", "session", "dwh"]
    connector.__aexit__.assert_awaited_once()


@pytest.mark.parametrize(
    "committed, initial, expected",
    [
        (-1001, -1001, 10),
        (0, 0, 10),
        (30, 30, 10),
        (10, 0, 10),
        (14, 0, 14),
        (20, 0, 20),
    ],
)
def test_fct_staging_positive_resume_offset(committed, initial, expected):
    """[unit] Replay honors the earliest reset recorded by staging and existing commits."""
    from src.pipelines import FctEventsPipeline

    dataset = Dataset(
        source="fct_events",
        dag_id="dag",
        run_id="run",
        task_id="stage",
        interval_start=NOW,
        interval_end=NOW,
        batch_count=1,
        row_count=1,
        first_offset=10,
        next_offset=20,
        committed_offset=initial,
    )
    assert FctEventsPipeline._resume_offset(dataset, committed) == expected


@pytest.mark.parametrize("committed", [0, 30])
def test_fct_staging_negative_unexpected_resume_offset(committed):
    """[unit] A changed commit outside the frozen range must not discard staged messages."""
    from src.pipelines import FctEventsPipeline

    dataset = Dataset(
        source="fct_events",
        dag_id="dag",
        run_id="run",
        task_id="stage",
        interval_start=NOW,
        interval_end=NOW,
        batch_count=1,
        row_count=1,
        first_offset=10,
        next_offset=20,
        committed_offset=10,
    )
    with pytest.raises(RuntimeError):
        FctEventsPipeline._resume_offset(dataset, committed)
