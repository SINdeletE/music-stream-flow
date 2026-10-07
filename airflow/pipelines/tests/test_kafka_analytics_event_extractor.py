import json
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.extractors.kafka_analytics_event_extractor import KafkaAnalyticsEventExtractor
from src.models.kafka.analytics_event import EventType

START = datetime(2026, 10, 5, tzinfo=UTC)
END = datetime(2026, 10, 6, tzinfo=UTC)


def message(offset=0, **fields):
    data = {
        "event_id": str(UUID(int=offset + 1)),
        "event_type": "track_selected",
        "device_id": str(UUID(int=2)),
        "device_os_id": str(UUID(int=3)),
        "app_release_id": str(UUID(int=4)),
        "session_id": str(UUID(int=5)),
        "event_timestamp": START.isoformat(),
        "received_at": START.isoformat(),
    } | fields
    return Mock(**{
        "error.return_value": None,
        "topic.return_value": "music-events",
        "partition.return_value": 0,
        "offset.return_value": offset,
        "value.return_value": json.dumps(data).encode(),
    })


class KafkaAnalyticsEventExtractorTests(unittest.IsolatedAsyncioTestCase):
    def extractor(self, messages):
        self.consumer = Mock(spec=Consumer)
        self.consumer.consume.return_value = messages
        settings = SimpleNamespace(music_kafka_batch_size=1000, music_kafka_timeout=1.0)
        return KafkaAnalyticsEventExtractor(self.consumer, settings)

    async def read(self, extractor, start=START, end=END):
        return [item async for item in extractor.read_batches(start, end)]

    async def test_partial_batch_converts_types_and_returns_next_offset(self):
        extractor = self.extractor([
            message(0, user_id=str(UUID(int=6)), track_id=str(UUID(int=7)), playback_id=str(UUID(int=8))),
            message(4, event_type="session_started", track_id=None),
        ])
        [(batch, next_offset)] = await self.read(extractor)
        self.assertEqual(next_offset, 5)
        self.assertEqual([event.event_id for event in batch], [UUID(int=1), UUID(int=5)])
        self.assertEqual(batch[0].event_type, EventType.track_selected)
        self.assertEqual(batch[0].device_id, UUID(int=2))
        self.assertEqual(batch[0].device_os_id, UUID(int=3))
        self.assertEqual(batch[0].app_release_id, UUID(int=4))
        self.assertEqual(batch[0].session_id, UUID(int=5))
        self.assertEqual(batch[0].user_id, UUID(int=6))
        self.assertEqual(batch[0].track_id, UUID(int=7))
        self.assertEqual(batch[0].playback_id, UUID(int=8))
        self.assertEqual(batch[0].event_timestamp, START)
        self.assertEqual(batch[0].received_at, START)
        self.assertIsNone(batch[1].user_id)
        self.assertIsNone(batch[1].track_id)
        self.assertIsNone(batch[1].playback_id)
        self.consumer.consume.assert_called_once_with(num_messages=1000, timeout=1.0)
        self.consumer.commit.assert_not_called()
        self.consumer.close.assert_not_called()

    async def test_empty_batch(self):
        self.assertEqual(await self.read(self.extractor([])), [])

    async def test_time_boundary_does_not_advance_past_deferred_message(self):
        for messages, expected_offset in (
            ([message(0, event_timestamp=END.isoformat())], None),
            ([message(0), message(1, event_timestamp=END.isoformat()), message(2)], 1),
        ):
            with self.subTest(expected_offset=expected_offset):
                result = await self.read(self.extractor(messages))
                if expected_offset is None:
                    self.assertEqual(result, [])
                else:
                    [(batch, next_offset)] = result
                    self.assertEqual(len(batch), 1)
                    self.assertEqual(next_offset, expected_offset)

    async def test_late_events_are_not_filtered_by_left_time_boundary(self):
        [(batch, next_offset)] = await self.read(self.extractor([
            message(0, event_timestamp=(START - timedelta(days=1)).isoformat()),
        ]))
        self.assertEqual(batch[0].event_timestamp, START - timedelta(days=1))
        self.assertEqual(next_offset, 1)

    async def test_invalid_messages(self):
        missing_payload = message()
        missing_payload.value.return_value = None
        invalid_json = message()
        invalid_json.value.return_value = b"invalid"
        non_object = message()
        non_object.value.return_value = b"[]"
        for messages in (
            [missing_payload], [invalid_json], [non_object],
            [message(event_type="unknown")], [message(device_id="invalid")],
            [message(event_timestamp=START.replace(tzinfo=None).isoformat())],
            [message(received_at=START.replace(tzinfo=None).isoformat())],
        ):
            with self.subTest(messages=messages):
                with self.assertRaises(ValueError):
                    await self.read(self.extractor(messages))

    async def test_kafka_and_consume_errors_propagate(self):
        failed_message = message()
        failed_message.error.return_value = KafkaError(KafkaError._TIMED_OUT)
        with self.assertRaises(KafkaException):
            await self.read(self.extractor([failed_message]))
        extractor = self.extractor([])
        error = RuntimeError("consume failed")
        self.consumer.consume.side_effect = error
        with self.assertRaises(RuntimeError) as caught:
            await self.read(extractor)
        self.assertIs(caught.exception, error)

    async def test_interval_validation_and_zero_interval(self):
        extractor = self.extractor([])
        for start, end in ((START.replace(tzinfo=None), END), (END, START)):
            with self.assertRaises(ValueError):
                await self.read(extractor, start, end)
        self.assertEqual(await self.read(extractor, START, START), [])
        self.consumer.consume.assert_not_called()
