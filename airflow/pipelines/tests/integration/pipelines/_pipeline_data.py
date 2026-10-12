import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import ModuleType
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import delete
from src.connections import OltpConnector
from src.loaders import (
    DimAppReleaseLoader,
    DimDeviceLoader,
    DimDeviceOsLoader,
    DimEventTypeLoader,
    DimGenreLoader,
    DimTrackLoader,
)
from src.models.dwh import (
    ClickHouseModel,
    DimAppRelease,
    DimDevice,
    DimDeviceOs,
    DimEventType,
    DimGenre,
    DimTrack,
    EtlLoad,
)
from src.models.oltp import Genre, Track, TrackGenre
from src.settings import KafkaSettings, OltpSettings

START = datetime(2026, 10, 11, 12, tzinfo=UTC)
END = START + timedelta(hours=1)
CREATED = START - timedelta(days=1)
SEED_LOAD_ID = UUID(int=9000)
DAG_ID = "integration_pipeline_dag"
RUN_ID = "integration_pipeline_run"
BATCH_SIZE = 4
PARTITION = 1
RECORD_COUNTS = (0, 8, 9)
RECORD_IDS = ("zero_records", "full_batches", "partial_batch")
NAIVE_INTERVALS = (
    (START.replace(tzinfo=None), END),
    (START, END.replace(tzinfo=None)),
    (START.replace(tzinfo=None), END.replace(tzinfo=None)),
)
NAIVE_IDS = ("naive_start", "naive_end", "both_naive")


def forbid_connections(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType
) -> list[Mock]:
    names = (
        ("DwhConnector", "KafkaConsumerConnector", "DwhSettings", "KafkaSettings")
        if module.__name__.endswith("fct_events_pipeline")
        else ("DwhConnector", "OltpConnector", "DwhSettings", "OltpSettings")
    )
    guards = []
    for name in names:
        guard = Mock(side_effect=AssertionError(f"Unexpected initialization: {name}"))
        monkeypatch.setattr(module, name, guard)
        guards.append(guard)
    return guards


async def clear_clickhouse(client: AsyncClient, *models: type[ClickHouseModel]) -> None:
    for model in (*models, EtlLoad):
        await client.command(f"TRUNCATE TABLE {model.table_to_insert()}")


def make_tracks(count: int) -> list[Track]:
    timestamps = [START + timedelta(milliseconds=index) for index in range(count)]
    timestamps.extend((START - timedelta(milliseconds=1), END))
    return [
        Track(
            id=UUID(int=1000 + index),
            name=f"Track №{index} 🎵",
            duration_ms=180000 + index,
            image_url=f"https://example.test/tracks/{index}.png" if index % 2 else None,
            is_available=bool(index % 2),
            created_at=CREATED,
            updated_at=timestamp,
            deleted_at=timestamp if index % 2 else None,
        )
        for index, timestamp in enumerate(timestamps)
    ]


async def seed_postgresql(
    settings: OltpSettings,
    tracks: list[Track],
    genres: list[Genre] | None = None,
    links: list[TrackGenre] | None = None,
) -> None:
    async with OltpConnector(settings) as connection, connection.session() as session:
        for model in (TrackGenre, Track, Genre):
            await session.execute(delete(model))
        session.add_all(tracks)
        session.add_all(genres or [])
        await session.flush()
        session.add_all(links or [])
        await session.commit()


async def seed_tracks(client: AsyncClient, tracks: list[Track]) -> None:
    await DimTrackLoader(client).load_batch(
        [
            DimTrack(
                track_sk=index * 10,
                track_id=track.id,
                name=f"Previous track {index}",
                duration_ms=100000,
                is_available=False,
                source_created_at=CREATED,
                updated_at=CREATED,
                deleted_at=None,
                load_id=SEED_LOAD_ID,
            )
            for index, track in enumerate(tracks)
        ]
    )


def expected_tracks(tracks: list[Track], count: int, load_id: UUID) -> list[dict]:
    return [
        {
            "track_sk": index * 10,
            "track_id": track.id,
            "name": track.name,
            "duration_ms": track.duration_ms,
            "is_available": track.is_available,
            "source_created_at": track.created_at,
            "updated_at": track.updated_at,
            "deleted_at": track.deleted_at,
            "load_id": load_id,
        }
        for index, track in enumerate(tracks[:count])
    ]


async def seed_genres(client: AsyncClient, tracks: list[Track]) -> list[Genre]:
    genres = [
        Genre(
            id=UUID(int=2000 + index),
            name=f"Genre {index}",
            created_at=CREATED,
            updated_at=CREATED,
            deleted_at=None,
        )
        for index in range(len(tracks))
    ]
    await DimGenreLoader(client).load_batch(
        [
            DimGenre(
                genre_sk=100 + index * 10,
                genre_id=genre.id,
                name=genre.name,
                source_created_at=genre.created_at,
                updated_at=genre.updated_at,
                deleted_at=None,
                load_id=SEED_LOAD_ID,
            )
            for index, genre in enumerate(genres)
        ]
    )
    return genres


async def seed_event_dimensions(client: AsyncClient) -> None:
    await DimDeviceLoader(client).load_batch(
        [
            DimDevice(
                device_sk=0,
                device_id=UUID(int=3000),
                name="Pixel 9",
                device_version="Pro",
                source_created_at=CREATED,
                updated_at=CREATED,
                load_id=SEED_LOAD_ID,
            )
        ]
    )
    await DimDeviceOsLoader(client).load_batch(
        [
            DimDeviceOs(
                device_os_sk=5,
                device_os_id=UUID(int=4000),
                name="Android",
                device_os_version="15",
                valid_from=CREATED,
                source_created_at=CREATED,
                load_id=SEED_LOAD_ID,
            )
        ]
    )
    await DimAppReleaseLoader(client).load_batch(
        [
            DimAppRelease(
                app_release_sk=7,
                app_release_id=UUID(int=5000),
                platform="android",
                app_release_version="1.2.3",
                build="123",
                released_at=CREATED,
                source_created_at=CREATED,
                load_id=SEED_LOAD_ID,
            )
        ]
    )
    await DimTrackLoader(client).load_batch(
        [
            DimTrack(
                track_sk=11,
                track_id=UUID(int=6000),
                name="Evening track 🎵",
                duration_ms=180000,
                is_available=True,
                source_created_at=CREATED,
                updated_at=CREATED,
                load_id=SEED_LOAD_ID,
            )
        ]
    )
    await DimEventTypeLoader(client).load_batch(
        [
            DimEventType(event_type_sk=0, name="session_started", load_id=SEED_LOAD_ID),
            DimEventType(event_type_sk=1, name="track_selected", load_id=SEED_LOAD_ID),
        ]
    )


def make_event_payloads(count: int) -> list[dict]:
    return [
        {
            "event_id": str(UUID(int=10000 + index)),
            "event_type": "track_selected" if index % 2 else "session_started",
            "device_id": str(UUID(int=3000)),
            "device_os_id": str(UUID(int=4000)),
            "app_release_id": str(UUID(int=5000)),
            "session_id": str(UUID(int=7000 + index % 3)),
            "user_id": str(UUID(int=8000 + index)) if index % 2 else None,
            "track_id": str(UUID(int=6000)) if index % 2 else None,
            "playback_id": str(UUID(int=11000 + index)) if index % 2 else None,
            "event_timestamp": (START + timedelta(milliseconds=index)).isoformat(),
            "received_at": (START + timedelta(milliseconds=index + 123)).isoformat(),
        }
        for index in range(count)
    ]


def expected_events(payloads: list[dict], load_id: UUID) -> list[dict]:
    return [
        {
            "event_sk": 0,
            "event_id": UUID(payload["event_id"]),
            "event_type_sk": 1 if index % 2 else 0,
            "device_sk": 0,
            "device_os_sk": 5,
            "app_release_sk": 7,
            "track_sk": 11 if index % 2 else None,
            "user_id": UUID(payload["user_id"]) if index % 2 else None,
            "playback_id": UUID(payload["playback_id"]) if index % 2 else None,
            "session_id": UUID(payload["session_id"]),
            "event_timestamp": datetime.fromisoformat(payload["event_timestamp"]),
            "received_at": datetime.fromisoformat(payload["received_at"]),
            "load_id": load_id,
        }
        for index, payload in enumerate(payloads)
    ]


def _prepare_topic(bootstrap_servers: str, topic: str, payloads: list[dict]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    future = admin.create_topics(
        [NewTopic(topic, num_partitions=3, replication_factor=1)],
        operation_timeout=30,
        request_timeout=30,
    )[topic]
    future.result(timeout=30)
    producer = Producer(
        {"bootstrap.servers": bootstrap_servers, "message.timeout.ms": 30000}
    )
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(error)

    for payload in payloads:
        producer.produce(
            topic,
            partition=PARTITION,
            value=json.dumps(payload).encode(),
            on_delivery=delivered,
        )
    remaining = producer.flush(timeout=30)
    assert remaining == 0
    assert errors == []


async def prepare_kafka(bootstrap_servers: str, payloads: list[dict]) -> KafkaSettings:
    topic = f"pipeline-events-{uuid4().hex}"
    await asyncio.to_thread(_prepare_topic, bootstrap_servers, topic, payloads)
    host, port = bootstrap_servers.rsplit(":", 1)
    settings = KafkaSettings()
    settings.music_kafka_host = host
    settings.music_kafka_port = port
    settings.music_kafka_topic_name = topic
    settings.music_kafka_num_partitions = 3
    settings.music_kafka_batch_size = BATCH_SIZE
    settings.music_kafka_timeout = 10.0
    return settings


async def assert_loaded_rows(
    client: AsyncClient,
    model: type[ClickHouseModel],
    expected: list[dict],
    load_id: UUID,
    *,
    key: str,
) -> None:
    result = await client.query(
        f"SELECT * FROM {model.table_to_insert()} WHERE load_id = {{load_id:UUID}}",
        parameters={"load_id": load_id},
        tz_mode="aware",
    )
    actual = list(result.named_results())
    assert len(actual) == len(expected)
    assert sorted(actual, key=lambda row: row[key]) == sorted(
        expected, key=lambda row: row[key]
    )


async def assert_audit(
    client: AsyncClient,
    audit: EtlLoad,
    count: int,
    *,
    task_id: str,
    target: type[ClickHouseModel],
) -> None:
    assert type(audit) is EtlLoad
    assert isinstance(audit.load_id, UUID)
    assert audit.load_id != SEED_LOAD_ID
    assert audit.state == "success"
    assert audit.rows_read == count
    assert audit.rows_written == count
    assert audit.rows_rejected == 0
    assert audit.error_msg is None
    assert audit.dag_id == DAG_ID
    assert audit.run_id == RUN_ID
    assert audit.task_id == task_id
    assert audit.source_system == "music_oltp"
    assert audit.target_table == target.table_to_insert()
    assert audit.interval_start == START
    assert audit.interval_end == END
    assert audit.started_at.utcoffset() == timedelta(0)
    assert audit.finished_at is not None
    assert audit.finished_at.utcoffset() == timedelta(0)
    assert audit.started_at <= audit.finished_at

    result = await client.query(
        "SELECT * FROM music_dwh.etl_loads WHERE load_id = {load_id:UUID}",
        parameters={"load_id": audit.load_id},
        tz_mode="aware",
    )
    rows = list(result.named_results())
    assert len(rows) == 2
    states = {row["state"]: row for row in rows}
    assert set(states) == {"running", "success"}
    common = {
        "load_id": audit.load_id,
        "dag_id": DAG_ID,
        "run_id": RUN_ID,
        "task_id": task_id,
        "source_system": "music_oltp",
        "target_table": target.table_to_insert(),
        "interval_start": START,
        "interval_end": END,
        "started_at": audit.started_at.replace(
            microsecond=audit.started_at.microsecond // 1000 * 1000
        ),
        "rows_rejected": 0,
        "error_msg": None,
    }
    assert states["running"] == {
        **common,
        "state": "running",
        "finished_at": None,
        "rows_read": 0,
        "rows_written": 0,
    }
    assert states["success"] == {
        **common,
        "state": "success",
        "finished_at": audit.finished_at.replace(
            microsecond=audit.finished_at.microsecond // 1000 * 1000
        ),
        "rows_read": count,
        "rows_written": count,
    }
