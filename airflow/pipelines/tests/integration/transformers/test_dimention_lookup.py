from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.loaders import (
    DimAppReleaseLoader,
    DimArtistLoader,
    DimDeviceLoader,
    DimDeviceOsLoader,
    DimGenreLoader,
    DimReleaseLoader,
    DimTrackLoader,
)
from src.transformers import resolve_dimension_sks

from tests.integration.fixtures.clickhouse import clickhouse_client

DIMENSIONS = (
    ("device", DimDeviceLoader, "updated_at"),
    ("device_os", DimDeviceOsLoader, "valid_from"),
    ("app_release", DimAppReleaseLoader, "source_created_at"),
    ("track", DimTrackLoader, "updated_at"),
    ("artist", DimArtistLoader, "updated_at"),
    ("genre", DimGenreLoader, "updated_at"),
    ("release", DimReleaseLoader, "updated_at"),
)


@pytest.fixture
def dimension_batches() -> dict[str, list[dict[str, object]]]:
    batches = {entity_name: [] for entity_name, _, _ in DIMENSIONS}
    for index in range(2):
        timestamp = datetime(2026, 10, 11, 12, 0, index, 123000, tzinfo=UTC)
        optional_timestamp = timestamp + timedelta(days=1) if index else None
        created = {
            "load_id": UUID(int=200 + index),
            "source_created_at": timestamp,
        }
        updated = {**created, "updated_at": timestamp + timedelta(seconds=1)}
        attributes = {
            "device": {
                **updated,
                "name": f"Device {index}",
                "device_version": f"v{index}",
            },
            "device_os": {
                **created,
                "name": f"OS {index}",
                "device_os_version": f"{index}.1",
                "valid_from": timestamp - timedelta(days=1),
            },
            "app_release": {
                **created,
                "platform": "ios" if index else "android",
                "app_release_version": f"1.{index}",
                "build": f"build-{index}",
                "released_at": timestamp - timedelta(days=2),
            },
            "track": {
                **updated,
                "name": f"Track {index} 🎵",
                "duration_ms": 180000 + index,
                "is_available": bool(index),
                "deleted_at": optional_timestamp,
            },
            "artist": {
                **updated,
                "nickname": f"Artist {index}",
                "name": "First name" if index else None,
                "middle_name": "Middle name" if index else None,
                "last_name": "Last name" if index else None,
                "country_code": "GB" if index else None,
                "biography": "Artist biography 🎸" if index else None,
                "is_verified": bool(index),
                "deleted_at": optional_timestamp,
            },
            "genre": {
                **updated,
                "name": f"Genre {index}",
                "deleted_at": optional_timestamp,
            },
            "release": {
                **updated,
                "name": f"Album {index}",
                "is_available": bool(index),
                "released_at": timestamp - timedelta(days=3),
                "deleted_at": optional_timestamp,
            },
        }
        for entity_name, values in attributes.items():
            batches[entity_name].append(
                {
                    f"{entity_name}_sk": index,
                    f"{entity_name}_id": UUID(int=100 + index),
                    **values,
                }
            )
    return batches


@pytest_asyncio.fixture(scope="function", loop_scope="function")
async def session() -> AsyncGenerator[AsyncClient, None]:
    async with clickhouse_client() as client:
        yield client


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize(
    ("entity_name", "loader_class", "version_column"),
    DIMENSIONS,
    ids=[entity_name for entity_name, _, _ in DIMENSIONS],
)
async def test_dimention_lookup(
    session: AsyncClient,
    dimension_batches: dict[str, list[dict[str, object]]],
    entity_name,
    loader_class,
    version_column,
):
    """[integration] Lookup находит UUID -> SK во всех семи измерениях, включая SK 0."""
    rows = dimension_batches[entity_name]
    table_name = loader_class.model.table_to_insert()
    await session.command(f"TRUNCATE TABLE {table_name}")
    loader = loader_class(session)
    written = await loader.load_batch([loader_class.model(**row) for row in rows])
    assert written == len(rows)

    expected = {row[f"{entity_name}_id"]: row[f"{entity_name}_sk"] for row in rows}
    actual = await resolve_dimension_sks(
        session,
        expected,
        table_name=table_name,
        entity_name=entity_name,
        version_column=version_column,
    )

    assert actual == expected
