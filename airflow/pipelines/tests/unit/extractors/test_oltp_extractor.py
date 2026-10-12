import unittest
from contextlib import aclosing
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy.ext.asyncio import AsyncSession
from src.extractors import TrackExtractor


class OltpExtractorTests(unittest.IsolatedAsyncioTestCase):
    async def test_oltp_extractor_positive_equal_timestamps(self):
        """[unit] Равные timestamps дают пустой результат без запроса к БД."""
        client = AsyncMock(spec=AsyncSession)
        settings = SimpleNamespace(music_oltp_batch_size=4)
        extractor = TrackExtractor(client, settings)
        timestamp = datetime.fromisoformat("2026-10-11T12:00:00+00:00")

        async with aclosing(extractor.read_batches(timestamp, timestamp)) as batches:
            actual_batches = [batch async for batch in batches]

        self.assertEqual(actual_batches, [])
        client.stream_scalars.assert_not_called()

    async def test_oltp_extractor_negative_start_bigger_than_end(self):
        """[unit] Начало позже конца вызывает ValueError без запроса к БД."""
        client = AsyncMock(spec=AsyncSession)
        settings = SimpleNamespace(music_oltp_batch_size=4)
        extractor = TrackExtractor(client, settings)
        timestamp_start = datetime.fromisoformat("2026-10-11T13:00:00+00:00")
        timestamp_end = datetime.fromisoformat("2026-10-11T12:00:00+00:00")

        with self.assertRaisesRegex(ValueError, "must not be after"):
            async with aclosing(
                extractor.read_batches(timestamp_start, timestamp_end)
            ) as batches:
                await anext(batches)

        client.stream_scalars.assert_not_called()

    async def test_oltp_extractor_negative_non_utcoffset_timestamps(self):
        """[unit] Даты без timezone вызывают ValueError без запроса к БД."""
        cases = [
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
        ]

        for timestamp_start, timestamp_end in cases:
            with self.subTest(
                timestamp_start=timestamp_start, timestamp_end=timestamp_end
            ):
                client = AsyncMock(spec=AsyncSession)
                settings = SimpleNamespace(music_oltp_batch_size=4)
                extractor = TrackExtractor(client, settings)

                with self.assertRaisesRegex(ValueError, "must be timezone-aware"):
                    async with aclosing(
                        extractor.read_batches(timestamp_start, timestamp_end)
                    ) as batches:
                        await anext(batches)

                client.stream_scalars.assert_not_called()
