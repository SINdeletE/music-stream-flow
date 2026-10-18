from contextlib import aclosing
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from src.extractors import (
    AppReleaseExtractor,
    ArtistExtractor,
    DeviceExtractor,
    DeviceOsExtractor,
    GenreExtractor,
    ReleaseExtractor,
    TrackExtractor,
)

EXTRACTORS = (
    DeviceExtractor,
    DeviceOsExtractor,
    AppReleaseExtractor,
    TrackExtractor,
    ArtistExtractor,
    GenreExtractor,
    ReleaseExtractor,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("extractor_class", EXTRACTORS)
@pytest.mark.parametrize("case", ["start", "end", "both", "reversed"])
async def test_dim_extractor_negative_invalid_interval(extractor_class, case):
    """[unit] Every dimension rejects invalid timestamps even when additional IDs are supplied."""
    start = datetime(2026, 10, 11, tzinfo=UTC)
    end = start + timedelta(hours=1)
    if case in ("start", "both"):
        start = start.replace(tzinfo=None)
    if case in ("end", "both"):
        end = end.replace(tzinfo=None)
    if case == "reversed":
        start, end = end, start
    session = AsyncMock(spec=AsyncSession)
    extractor = extractor_class(session, SimpleNamespace(music_oltp_batch_size=4))
    with pytest.raises(ValueError):
        async with aclosing(
            extractor.read_batches(start, end, missing_ids={UUID(int=1)})
        ) as batches:
            await anext(batches)
    session.stream_scalars.assert_not_called()
