import unittest
from contextlib import aclosing
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncScalarResult, AsyncSession
from sqlalchemy.orm import Session
from src.extractors import TrackExtractor
from src.models.oltp import Track


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


class OltpExtractorIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_oltp_extractor_positive_records(self):
        """[integration] SQLite фильтрует записи по [start, end) и сортирует их."""
        cases = [(20, 4), (19, 4), (0, 4), (1, 4), (5, 4), (5, 1), (5, 10)]
        timestamp_start = datetime.fromisoformat("2026-10-11T12:00:00+00:00")
        timestamp_end = datetime.fromisoformat("2026-10-11T13:00:00+00:00")

        for count, batch_size in cases:
            with self.subTest(count=count, batch_size=batch_size):
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
                engine = create_engine(
                    "sqlite:///:memory:",
                    execution_options={"schema_translate_map": {"music_oltp": None}},
                )
                self.addCleanup(engine.dispose)
                Track.__table__.create(engine)

                with Session(engine) as database_session:
                    database_session.add_all(source)
                    database_session.commit()

                    client = AsyncMock(spec=AsyncSession)
                    result = Mock(spec=AsyncScalarResult)
                    result.close = AsyncMock()

                    async def stream_scalars(
                        statement, *, database_session=database_session, result=result
                    ):
                        rows = database_session.scalars(statement).all()

                        async def partitions(size):
                            for start in range(0, len(rows), size):
                                yield rows[start : start + size]

                        result.partitions.side_effect = partitions
                        return result

                    client.stream_scalars.side_effect = stream_scalars
                    settings = SimpleNamespace(music_oltp_batch_size=batch_size)
                    extractor = TrackExtractor(client, settings)

                    actual_ids = []
                    actual_batch_sizes = []
                    async with aclosing(
                        extractor.read_batches(timestamp_start, timestamp_end)
                    ) as batches:
                        async for batch in batches:
                            self.assertIsInstance(batch, list)
                            self.assertTrue(batch)
                            self.assertLessEqual(len(batch), batch_size)
                            actual_ids.extend(entity.id for entity in batch)
                            actual_batch_sizes.append(len(batch))

                    self.assertEqual(actual_ids, expected_ids)
                    self.assertEqual(
                        actual_batch_sizes,
                        [
                            min(batch_size, count - start)
                            for start in range(0, count, batch_size)
                        ],
                    )
                    client.stream_scalars.assert_awaited_once()
                    statement = client.stream_scalars.await_args.args[0]
                    self.assertEqual(
                        statement.get_execution_options()["yield_per"], batch_size
                    )
                    result.partitions.assert_called_once_with(batch_size)
                    result.close.assert_awaited_once()
