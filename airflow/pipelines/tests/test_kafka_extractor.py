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
    async def test_kafka_extractor_records(self):
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
