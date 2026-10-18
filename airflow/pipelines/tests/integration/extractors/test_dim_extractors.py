from contextlib import aclosing
from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from src.extractors import (
    AppReleaseExtractor,
    ArtistExtractor,
    DeviceExtractor,
    DeviceOsExtractor,
    GenreExtractor,
    ReleaseExtractor,
    TrackExtractor,
)
from src.models.oltp import Base

from tests.integration.transformers._transformer_data import (
    TIMESTAMP,
    make_dimension_batch,
)

EXTRACTORS = {
    "device": DeviceExtractor,
    "device_os": DeviceOsExtractor,
    "app_release": AppReleaseExtractor,
    "track": TrackExtractor,
    "artist": ArtistExtractor,
    "genre": GenreExtractor,
    "release": ReleaseExtractor,
}


@pytest.mark.asyncio
@pytest.mark.parametrize("dimension", EXTRACTORS)
@pytest.mark.parametrize("equal", [False, True], ids=["interval", "equal"])
async def test_dim_extractor_positive_missing_ids(dimension, equal):
    """[integration] Every dimension selects the time window OR missing IDs exactly once."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        execution_options={"schema_translate_map": {"music_oltp": None}},
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            extractor_class = EXTRACTORS[dimension]
            model = extractor_class.model
            timestamp_name = extractor_class.timestamp_column.key
            start = TIMESTAMP
            end = start if equal else start + timedelta(hours=1)
            timestamps = [
                start - timedelta(seconds=1),
                start,
                start + timedelta(seconds=1),
                start + timedelta(hours=1),
                start + timedelta(hours=2),
            ]
            template = make_dimension_batch(dimension)[0][0]
            values = {
                column.key: getattr(template, column.key)
                for column in inspect(model).columns
            }
            values = {
                key: UUID(int=(0xAAAA << 112) | value.int)
                if isinstance(value, UUID)
                else value
                for key, value in values.items()
            }
            records = [
                model(
                    **{
                        **values,
                        "id": UUID(int=(0xAAAA << 112) | (10000 + i)),
                        timestamp_name: timestamp,
                    }
                )
                for i, timestamp in enumerate(timestamps)
            ]
            session.add_all(list(reversed(records)))
            await session.commit()
            extras = [
                records[0].id,
                records[2].id,
                records[3].id,
                records[0].id,
                UUID(int=99999),
            ]
            settings = SimpleNamespace(music_oltp_batch_size=19)
            extractor = extractor_class(session, settings, batch_size=2)
            batches = []
            async with aclosing(
                extractor.read_batches(start, end, missing_ids=extras)
            ) as stream:
                async for batch in stream:
                    batches.append(batch)
            expected = [0, 2, 3] if equal else [0, 1, 2, 3]
            assert [row.id for batch in batches for row in batch] == [
                records[i].id for i in expected
            ]
            assert all(0 < len(batch) <= 2 for batch in batches)
            assert settings.music_oltp_batch_size == 19
    finally:
        await engine.dispose()
