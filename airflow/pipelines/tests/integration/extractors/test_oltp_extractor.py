from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from src.extractors import TrackExtractor
from src.models.oltp import Track

from tests.integration.fixtures.sqlite import sqlite_session


def make_track(identifier: int, updated_at: datetime) -> Track:
    return Track(
        id=UUID(f"aaaaaaaa-0000-0000-0000-{identifier:012x}"),
        name=f"Track {identifier}",
        duration_ms=180000,
        image_url=None,
        is_available=True,
        created_at=updated_at,
        updated_at=updated_at,
        deleted_at=None,
    )


@pytest_asyncio.fixture(scope="function", loop_scope="function")
async def session() -> AsyncGenerator[AsyncSession, None]:
    async with sqlite_session() as session:
        yield session


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize(
    ("count", "batch_size"),
    [(20, 4), (19, 4), (0, 4), (1, 4), (5, 4), (5, 1), (5, 10)],
)
async def test_oltp_extractor_positive_records(
    session: AsyncSession, count: int, batch_size: int
):
    """[integration] SQLite фильтрует записи по [start, end) и сортирует их."""
    timestamp_start = datetime.fromisoformat("2026-10-11T12:00:00+00:00")
    timestamp_end = datetime.fromisoformat("2026-10-11T13:00:00+00:00")
    matching = [
        make_track(i + 1, timestamp_start + timedelta(minutes=i % 3))
        for i in range(count)
    ]
    outside_interval = [
        make_track(1001, timestamp_start - timedelta(milliseconds=1)),
        make_track(1002, timestamp_end),
        make_track(1003, timestamp_end + timedelta(milliseconds=1)),
    ]
    expected_ids = [
        entity.id
        for entity in sorted(
            matching, key=lambda entity: (entity.updated_at, entity.id)
        )
    ]
    source = list(reversed(matching + outside_interval))
    session.add_all(source)
    await session.commit()

    settings = SimpleNamespace(music_oltp_batch_size=batch_size)
    extractor = TrackExtractor(session, settings)

    actual_ids = []
    actual_batch_sizes = []
    async with aclosing(
        extractor.read_batches(timestamp_start, timestamp_end)
    ) as batches:
        async for batch in batches:
            assert isinstance(batch, list)
            assert batch
            assert len(batch) <= batch_size
            actual_ids.extend(entity.id for entity in batch)
            actual_batch_sizes.append(len(batch))

    assert actual_ids == expected_ids
    assert actual_batch_sizes == [
        min(batch_size, count - start) for start in range(0, count, batch_size)
    ]
