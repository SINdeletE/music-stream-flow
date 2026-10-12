from contextlib import aclosing
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from src.extractors import TrackExtractor


@pytest.mark.asyncio
async def test_oltp_extractor_positive_equal_timestamps():
    """[unit] Равные timestamps дают пустой результат без запроса к БД."""
    client = AsyncMock(spec=AsyncSession)
    settings = SimpleNamespace(music_oltp_batch_size=4)
    extractor = TrackExtractor(client, settings)
    timestamp = datetime.fromisoformat("2026-10-11T12:00:00+00:00")

    async with aclosing(extractor.read_batches(timestamp, timestamp)) as batches:
        actual_batches = [batch async for batch in batches]

    assert actual_batches == []
    client.stream_scalars.assert_not_called()


@pytest.mark.asyncio
async def test_oltp_extractor_negative_start_bigger_than_end():
    """[unit] Начало позже конца вызывает ValueError без запроса к БД."""
    client = AsyncMock(spec=AsyncSession)
    settings = SimpleNamespace(music_oltp_batch_size=4)
    extractor = TrackExtractor(client, settings)
    timestamp_start = datetime.fromisoformat("2026-10-11T13:00:00+00:00")
    timestamp_end = datetime.fromisoformat("2026-10-11T12:00:00+00:00")

    with pytest.raises(ValueError, match="must not be after"):
        async with aclosing(
            extractor.read_batches(timestamp_start, timestamp_end)
        ) as batches:
            await anext(batches)

    client.stream_scalars.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("timestamp_start", "timestamp_end"),
    [
        (
            datetime.fromisoformat("2026-10-11T12:00:00"),
            datetime.fromisoformat("2026-10-11T13:00:00+00:00"),
        ),
        (
            datetime.fromisoformat("2026-10-11T12:00:00+00:00"),
            datetime.fromisoformat("2026-10-11T13:00:00"),
        ),
        (
            datetime.fromisoformat("2026-10-11T12:00:00"),
            datetime.fromisoformat("2026-10-11T13:00:00"),
        ),
    ],
    ids=["no_utcoffset_start", "no_utcoffset_end", "both_no_utcoffset"],
)
async def test_oltp_extractor_negative_non_utcoffset_timestamps(
    timestamp_start: datetime, timestamp_end: datetime
):
    """[unit] Даты без timezone вызывают ValueError без запроса к БД."""
    client = AsyncMock(spec=AsyncSession)
    settings = SimpleNamespace(music_oltp_batch_size=4)
    extractor = TrackExtractor(client, settings)

    with pytest.raises(ValueError, match="must be timezone-aware"):
        async with aclosing(
            extractor.read_batches(timestamp_start, timestamp_end)
        ) as batches:
            await anext(batches)

    client.stream_scalars.assert_not_called()
