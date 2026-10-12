from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import pytest_asyncio
from clickhouse_connect.driver import AsyncClient
from src.loaders import (
    BrgArtistReleaseLoader,
    BrgDeviceAppReleaseLoader,
    BrgTrackArtistLoader,
    BrgTrackGenreLoader,
    BrgTrackReleaseLoader,
    ClickHouseLoader,
    DimAppReleaseLoader,
    DimArtistLoader,
    DimDeviceLoader,
    DimDeviceOsLoader,
    DimEventTypeLoader,
    DimGenreLoader,
    DimReleaseLoader,
    DimTrackLoader,
    EtlLoadLoader,
    FctEventLoader,
)

from tests.integration.fixtures.clickhouse import clickhouse_client

LOADERS = (
    DimDeviceLoader,
    DimDeviceOsLoader,
    DimAppReleaseLoader,
    DimTrackLoader,
    DimArtistLoader,
    DimGenreLoader,
    DimReleaseLoader,
    DimEventTypeLoader,
    BrgDeviceAppReleaseLoader,
    BrgTrackGenreLoader,
    BrgTrackArtistLoader,
    BrgTrackReleaseLoader,
    BrgArtistReleaseLoader,
    FctEventLoader,
    EtlLoadLoader,
)


def make_loader_batches() -> dict[type[ClickHouseLoader], list[dict[str, object]]]:
    cases = {loader: [] for loader in LOADERS}
    for index in range(2):
        timestamp = datetime(2026, 10, 11, 12, 0, index, 123000, tzinfo=UTC)
        optional_timestamp = timestamp + timedelta(days=1) if index else None
        entity_id = UUID(int=100 + index)
        load_id = UUID(int=200 + index)
        audit = {"load_id": load_id}
        created = {**audit, "source_created_at": timestamp}
        updated = {**created, "updated_at": timestamp + timedelta(seconds=1)}
        cases[DimDeviceLoader].append(
            {
                **updated,
                "device_sk": index,
                "device_id": entity_id,
                "name": f"Device №{index}",
                "device_version": f"v{index}",
            }
        )
        cases[DimDeviceOsLoader].append(
            {
                **created,
                "device_os_sk": index,
                "device_os_id": entity_id,
                "name": f"OS №{index}",
                "device_os_version": f"{index}.1",
                "valid_from": timestamp - timedelta(days=1),
            }
        )
        cases[DimAppReleaseLoader].append(
            {
                **created,
                "app_release_sk": index,
                "app_release_id": entity_id,
                "platform": "ios" if index else "android",
                "app_release_version": f"1.{index}",
                "build": f"build-{index}",
                "released_at": timestamp - timedelta(days=2),
            }
        )
        cases[DimTrackLoader].append(
            {
                **updated,
                "track_sk": index,
                "track_id": entity_id,
                "name": f"Track №{index} 🎵",
                "duration_ms": 180000 + index,
                "is_available": bool(index),
                "deleted_at": optional_timestamp,
            }
        )
        cases[DimArtistLoader].append(
            {
                **updated,
                "artist_sk": index,
                "artist_id": entity_id,
                "nickname": f"Artist №{index}",
                "name": "First name" if index else None,
                "middle_name": "Middle name" if index else None,
                "last_name": "Last name" if index else None,
                "country_code": "RU" if index else None,
                "biography": "Artist biography 🎸" if index else None,
                "is_verified": bool(index),
                "deleted_at": optional_timestamp,
            }
        )
        cases[DimGenreLoader].append(
            {
                **updated,
                "genre_sk": index,
                "genre_id": entity_id,
                "name": f"Genre №{index}",
                "deleted_at": optional_timestamp,
            }
        )
        cases[DimReleaseLoader].append(
            {
                **updated,
                "release_sk": index,
                "release_id": entity_id,
                "name": f"Album №{index}",
                "is_available": bool(index),
                "released_at": timestamp - timedelta(days=3),
                "deleted_at": optional_timestamp,
            }
        )
        cases[DimEventTypeLoader].append(
            {
                **audit,
                "event_type_sk": index,
                "name": "session_started" if index else "track_selected",
                "deleted_at": optional_timestamp,
            }
        )
        cases[BrgDeviceAppReleaseLoader].append(
            {**audit, "device_sk": index, "app_release_sk": index + 10}
        )
        cases[BrgTrackGenreLoader].append(
            {**audit, "track_sk": index, "genre_sk": index + 20}
        )
        cases[BrgTrackArtistLoader].append(
            {
                **audit,
                "track_sk": index,
                "artist_sk": index + 30,
                "artist_number": index + 1,
            }
        )
        cases[BrgTrackReleaseLoader].append(
            {
                **audit,
                "track_sk": index,
                "release_sk": index + 40,
                "track_number": index + 1,
                "disc_number": index + 1 if index else None,
            }
        )
        cases[BrgArtistReleaseLoader].append(
            {
                **audit,
                "artist_sk": index,
                "release_sk": index + 50,
                "artist_number": index + 1,
            }
        )
        cases[FctEventLoader].append(
            {
                **audit,
                "event_sk": index,
                "event_id": entity_id,
                "user_id": UUID(int=300 + index) if index else None,
                "event_type_sk": index + 10,
                "device_sk": index + 20,
                "device_os_sk": index + 30,
                "app_release_sk": index + 40,
                "track_sk": index + 50 if index else None,
                "session_id": UUID(int=400 + index),
                "playback_id": UUID(int=500 + index) if index else None,
                "event_timestamp": timestamp,
                "received_at": timestamp + timedelta(milliseconds=456),
            }
        )
        cases[EtlLoadLoader].append(
            {
                **audit,
                "dag_id": f"test_dag_{index}",
                "run_id": f"test_run_{index}",
                "task_id": f"test_task_{index}",
                "source_system": "music_oltp",
                "target_table": f"music_dwh.table_{index}",
                "interval_start": timestamp - timedelta(hours=1),
                "interval_end": timestamp,
                "started_at": timestamp + timedelta(seconds=1),
                "finished_at": optional_timestamp,
                "state": "success" if index else "running",
                "rows_read": 10 + index,
                "rows_written": 8 + index,
                "rows_rejected": index,
                "error_msg": "Test message" if index else None,
            }
        )
    return cases


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def client():
    async with clickhouse_client() as client:
        yield client


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    ("loader_class", "expected_rows"), make_loader_batches().items()
)
async def test_clickhouse_loaders_positive_records(
    client: AsyncClient, loader_class, expected_rows
):
    """[integration] Loader-ы сохраняют батчи в ClickHouse без изменения значений."""

    batch = [loader_class.model(**values) for values in expected_rows]
    loader = loader_class(client)
    table = loader_class.model.table_to_insert()
    await client.command(f"TRUNCATE TABLE {table}")

    rows_written = await loader.load_batch(batch)

    assert rows_written == len(expected_rows)
    count = await client.query(f"SELECT count() FROM {table}")
    assert count.result_rows[0][0] == len(expected_rows)
    result = await client.query(f"SELECT * FROM {table}", tz_mode="aware")
    actual_rows = [
        dict(zip(result.column_names, row, strict=True)) for row in result.result_rows
    ]
    assert sorted(actual_rows, key=lambda x: int(x["load_id"])) == sorted(
        expected_rows, key=lambda x: int(x["load_id"])
    )
