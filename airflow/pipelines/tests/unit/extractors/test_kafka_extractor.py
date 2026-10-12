import json
import unittest
from contextlib import aclosing
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

from confluent_kafka import Consumer, Message, TopicPartition
from src.extractors import KafkaAnalyticsEventExtractor


def make_message(offset: int) -> Mock:
    data = {
        "event_id": str(UUID(int=100 + offset)),
        "event_type": "track_selected",
        "device_id": str(UUID(int=1)),
        "device_os_id": str(UUID(int=2)),
        "app_release_id": str(UUID(int=3)),
        "session_id": str(UUID(int=4)),
        "track_id": str(UUID(int=5)),
        "user_id": None,
        "playback_id": None,
        "event_timestamp": "2026-10-11T12:00:00+00:00",
        "received_at": "2026-10-11T12:00:01+00:00",
    }

    message = Mock(spec=Message)
    message.error.return_value = None
    message.offset.return_value = offset
    message.value.return_value = json.dumps(data).encode()
    return message


class KafkaAnalyticsEventExtractorTests(unittest.IsolatedAsyncioTestCase):
    async def test_kafka_extractor_positive_records(self):
        """[unit] Kafka возвращает батчи заданного размера и ожидаемые offsets."""
        cases = [(20, 4), (19, 4), (0, 4), (1, 4), (5, 4), (5, 1), (5, 10)]

        for cnt, batch_size in cases:
            with self.subTest(cnt=cnt, batch_size=batch_size):
                consumer = Mock(spec=Consumer)
                settings = SimpleNamespace(
                    music_kafka_topic_name="music-events",
                    music_kafka_batch_size=batch_size,
                    music_kafka_timeout=1.0,
                )
                partition = TopicPartition(settings.music_kafka_topic_name, 0, 0)
                consumer.assignment.return_value = [partition]

                count = cnt
                consumer.consume.side_effect = [
                    [
                        make_message(i)
                        for i in range(
                            start, min(start + settings.music_kafka_batch_size, count)
                        )
                    ]
                    for start in range(0, count, settings.music_kafka_batch_size)
                ]
                consumer.get_watermark_offsets.return_value = (0, count)
                consumer.consume.return_value = [make_message(i) for i in range(count)]
                consumer.position.side_effect = [
                    [TopicPartition("music-events", 0, offset)]
                    for offset in [
                        i for i in range(0, count, settings.music_kafka_batch_size)
                    ]
                ]

                extractor = KafkaAnalyticsEventExtractor(consumer, settings)

                timestamp_start = datetime.fromisoformat("2026-10-11T12:00:00+00:00")
                timestamp_end = datetime.fromisoformat("2026-10-11T13:00:00+00:00")

                expected_offsets = [
                    min(start + settings.music_kafka_batch_size, count)
                    for start in range(0, count, settings.music_kafka_batch_size)
                ]
                fact_offsets = []

                iteration_offset = 0
                async with aclosing(
                    extractor.read_batches(
                        timestamp_start=timestamp_start, timestamp_end=timestamp_end
                    ),
                ) as batches:
                    async for source_batch, next_offset in batches:
                        self.assertLessEqual(
                            len(source_batch), settings.music_kafka_batch_size
                        )
                        fact_offsets.append(next_offset)

                        iteration_offset += settings.music_kafka_batch_size

                self.assertEqual(fact_offsets, expected_offsets)

    async def test_kafka_extractor_positive_equal_timestamps(self):
        """[unit] Равные timestamps дают пустой результат чтения Kafka."""
        cases = [(20, 4), (19, 4), (0, 4), (1, 4), (5, 4)]

        for cnt, batch_size in cases:
            with self.subTest(cnt=cnt, batch_size=batch_size):
                consumer = Mock(spec=Consumer)
                settings = SimpleNamespace(
                    music_kafka_topic_name="music-events",
                    music_kafka_batch_size=batch_size,
                    music_kafka_timeout=1.0,
                )
                partition = TopicPartition(settings.music_kafka_topic_name, 0, 0)
                consumer.assignment.return_value = [partition]

                count = cnt
                consumer.consume.side_effect = [
                    [
                        make_message(i)
                        for i in range(
                            start, min(start + settings.music_kafka_batch_size, count)
                        )
                    ]
                    for start in range(0, count, settings.music_kafka_batch_size)
                ]
                consumer.get_watermark_offsets.return_value = (0, count)
                consumer.consume.return_value = [make_message(i) for i in range(count)]
                consumer.position.side_effect = [
                    [TopicPartition("music-events", 0, offset)]
                    for offset in [
                        i for i in range(0, count, settings.music_kafka_batch_size)
                    ]
                ]

                extractor = KafkaAnalyticsEventExtractor(consumer, settings)

                timestamp_start = datetime.fromisoformat("2026-10-11T12:00:00+00:00")
                timestamp_end = timestamp_start

                expected_offsets = []
                fact_offsets = []

                iteration_offset = 0
                async with aclosing(
                    extractor.read_batches(
                        timestamp_start=timestamp_start, timestamp_end=timestamp_end
                    ),
                ) as batches:
                    async for source_batch, next_offset in batches:
                        self.assertLessEqual(
                            len(source_batch), settings.music_kafka_batch_size
                        )
                        fact_offsets.append(next_offset)

                        iteration_offset += settings.music_kafka_batch_size

                self.assertEqual(fact_offsets, expected_offsets)

    async def test_kafka_extractor_negative_start_bigger_than_end(self):
        """[unit] Начало позже конца интервала не вызывает чтение Kafka."""
        consumer = Mock(spec=Consumer)
        settings = SimpleNamespace(
            music_kafka_topic_name="music-events",
            music_kafka_batch_size=4,
            music_kafka_timeout=1.0,
        )

        extractor = KafkaAnalyticsEventExtractor(consumer, settings)

        timestamp_start = datetime.fromisoformat("2026-10-11T13:00:00+00:00")
        timestamp_end = datetime.fromisoformat("2026-10-11T12:00:00+00:00")

        try:
            async with aclosing(
                extractor.read_batches(
                    timestamp_start=timestamp_start, timestamp_end=timestamp_end
                ),
            ) as batches:
                await anext(batches)

        except ValueError:
            pass

        consumer.consume.assert_not_called()

    async def test_kafka_extractor_negative_non_utcoffset_timestamps(self):
        """[unit] Даты без timezone не вызывают чтение Kafka."""
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

        for t_start, t_end in cases:
            with self.subTest(t_start=t_start, t_end=t_end):
                consumer = Mock(spec=Consumer)
                settings = SimpleNamespace(
                    music_kafka_topic_name="music-events",
                    music_kafka_batch_size=4,
                    music_kafka_timeout=1.0,
                )

                extractor = KafkaAnalyticsEventExtractor(consumer, settings)

                timestamp_start = t_start
                timestamp_end = t_end

                try:
                    async with aclosing(
                        extractor.read_batches(
                            timestamp_start=timestamp_start, timestamp_end=timestamp_end
                        ),
                    ) as batches:
                        await anext(batches)

                except ValueError:
                    pass

                consumer.consume.assert_not_called()
