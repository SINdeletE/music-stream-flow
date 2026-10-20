import json
import logging
from contextlib import aclosing
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cnt", "batch_size"),
    [(20, 4), (19, 4), (0, 4), (1, 4), (5, 4), (5, 1), (5, 10)],
)
async def test_kafka_extractor_positive_records(
    cnt: int, batch_size: int, caplog: pytest.LogCaptureFixture
):
    """[unit] Kafka возвращает батчи заданного размера и ожидаемые offsets."""
    caplog.set_level(
        logging.INFO, logger="src.extractors.kafka_analytics_event_extractor"
    )
    consumer = Mock(spec=Consumer)
    settings = SimpleNamespace(
        music_kafka_topic_name="music-events",
        music_kafka_batch_size=batch_size,
        music_kafka_timeout=1.0,
    )
    partition = TopicPartition(settings.music_kafka_topic_name, 0, 0)
    consumer.assignment.return_value = [partition]
    consumer.consume.side_effect = [
        [make_message(i) for i in range(start, min(start + batch_size, cnt))]
        for start in range(0, cnt, batch_size)
    ]
    consumer.get_watermark_offsets.return_value = (0, cnt)
    consumer.position.side_effect = [
        [TopicPartition(settings.music_kafka_topic_name, 0, offset)]
        for offset in range(0, cnt, batch_size)
    ]

    extractor = KafkaAnalyticsEventExtractor(consumer, settings)
    timestamp_start = datetime.fromisoformat("2026-10-11T12:00:00+00:00")
    timestamp_end = datetime.fromisoformat("2026-10-11T13:00:00+00:00")
    expected_offsets = [
        min(start + batch_size, cnt) for start in range(0, cnt, batch_size)
    ]
    fact_offsets = []
    event_ids = []

    async with aclosing(
        extractor.read_batches(
            timestamp_start=timestamp_start, timestamp_end=timestamp_end
        ),
    ) as batches:
        async for source_batch, next_offset in batches:
            assert len(source_batch) == min(batch_size, cnt - len(event_ids))
            fact_offsets.append(next_offset)
            event_ids.extend(event.event_id for event in source_batch)

    assert fact_offsets == expected_offsets
    assert event_ids == [UUID(int=100 + offset) for offset in range(cnt)]
    assert consumer.consume.call_count == len(expected_offsets)
    assert "Kafka reading started" in caplog.text
    assert "Kafka offset boundaries: topic=music-events partition=0" in caplog.text
    assert sum(
        record.getMessage().startswith("Kafka batch ready") for record in caplog.records
    ) == len(expected_offsets)
    assert "Kafka reading finished" in caplog.text
    assert all(str(event_id) not in caplog.text for event_id in event_ids)


@pytest.mark.asyncio
async def test_kafka_extractor_positive_empty_poll_logging(
    caplog: pytest.LogCaptureFixture,
):
    """[unit] Пустые ответы и возобновление чтения логируются без изменения батча."""
    caplog.set_level(
        logging.DEBUG, logger="src.extractors.kafka_analytics_event_extractor"
    )
    consumer = Mock(spec=Consumer)
    settings = SimpleNamespace(
        music_kafka_topic_name="music-events",
        music_kafka_batch_size=4,
        music_kafka_timeout=1.0,
    )
    partition = TopicPartition(settings.music_kafka_topic_name, 1, 0)
    consumer.assignment.return_value = [partition]
    consumer.get_watermark_offsets.return_value = (0, 1)
    consumer.position.return_value = [partition]
    consumer.consume.side_effect = [[], [], [make_message(0)]]
    extractor = KafkaAnalyticsEventExtractor(consumer, settings)

    async with aclosing(
        extractor.read_batches(
            timestamp_start=datetime.fromisoformat("2026-10-11T12:00:00+00:00"),
            timestamp_end=datetime.fromisoformat("2026-10-11T13:00:00+00:00"),
        )
    ) as batches:
        result = [batch async for batch in batches]

    assert len(result) == 1
    batch, next_offset = result[0]
    assert [event.event_id for event in batch] == [UUID(int=100)]
    assert next_offset == 1
    assert consumer.consume.call_count == 3
    empty_logs = [
        record
        for record in caplog.records
        if record.getMessage().startswith("Kafka poll returned no messages")
    ]
    assert [record.levelno for record in empty_logs] == [
        logging.WARNING,
        logging.DEBUG,
    ]
    assert "topic=music-events partition=1 position_before_poll=0 end_offset=1" in (
        empty_logs[0].getMessage()
    )
    assert "Kafka messages received after empty polls" in caplog.text
    assert "empty_polls=2 messages=1" in caplog.text
    assert str(UUID(int=100)) not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary", "reason"),
    [
        ("position", "position reached end_offset"),
        ("message_offset", "message reached end_offset"),
        ("timestamp", "timestamp boundary reached"),
        ("partition_eof", "partition EOF"),
    ],
)
async def test_kafka_extractor_positive_boundary_logging(
    boundary: str, reason: str, caplog: pytest.LogCaptureFixture
):
    """[unit] Причина завершения логируется, частичный батч сохраняется."""
    caplog.set_level(
        logging.INFO, logger="src.extractors.kafka_analytics_event_extractor"
    )
    consumer = Mock(spec=Consumer)
    settings = SimpleNamespace(
        music_kafka_topic_name="music-events",
        music_kafka_batch_size=4,
        music_kafka_timeout=1.0,
    )
    consumer.assignment.return_value = [
        TopicPartition(settings.music_kafka_topic_name, 1)
    ]
    consumer.get_watermark_offsets.return_value = (0, 2)
    consumer.position.return_value = [
        TopicPartition(
            settings.music_kafka_topic_name, 1, 2 if boundary == "position" else 0
        )
    ]
    timestamp_start = datetime.fromisoformat("2026-10-11T12:00:00+00:00")
    timestamp_end = datetime.fromisoformat("2026-10-11T13:00:00+00:00")
    terminating_message = make_message(2)
    if boundary == "timestamp":
        terminating_message = make_message(1)
        data = json.loads(terminating_message.value())
        data["event_timestamp"] = timestamp_end.isoformat()
        terminating_message.value.return_value = json.dumps(data).encode()
    elif boundary == "partition_eof":
        terminating_message.error.return_value = KafkaError(KafkaError._PARTITION_EOF)
    consumer.consume.return_value = [make_message(0), terminating_message]
    extractor = KafkaAnalyticsEventExtractor(consumer, settings)

    async with aclosing(
        extractor.read_batches(timestamp_start, timestamp_end)
    ) as batches:
        result = [batch async for batch in batches]

    if boundary == "position":
        assert result == []
        consumer.consume.assert_not_called()
    else:
        assert len(result) == 1
        batch, next_offset = result[0]
        assert [event.event_id for event in batch] == [UUID(int=100)]
        assert next_offset == 1
        consumer.consume.assert_called_once()
        assert "batch_rows=1 next_offset=1" in caplog.text
    assert f"Kafka reading finished: {reason}" in caplog.text
    assert "topic=music-events partition=1" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cnt", "batch_size"),
    [(20, 4), (19, 4), (0, 4), (1, 4), (5, 4)],
)
async def test_kafka_extractor_positive_equal_timestamps(cnt: int, batch_size: int):
    """[unit] Равные timestamps дают пустой результат чтения Kafka."""
    consumer = Mock(spec=Consumer)
    consumer.get_watermark_offsets.return_value = (0, cnt)
    settings = SimpleNamespace(
        music_kafka_topic_name="music-events",
        music_kafka_batch_size=batch_size,
        music_kafka_timeout=1.0,
    )
    extractor = KafkaAnalyticsEventExtractor(consumer, settings)
    timestamp = datetime.fromisoformat("2026-10-11T12:00:00+00:00")

    async with aclosing(
        extractor.read_batches(timestamp_start=timestamp, timestamp_end=timestamp),
    ) as batches:
        result = [batch async for batch in batches]

    assert result == []
    consumer.assignment.assert_not_called()
    consumer.get_watermark_offsets.assert_not_called()
    consumer.consume.assert_not_called()


@pytest.mark.asyncio
async def test_kafka_extractor_negative_start_bigger_than_end():
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

    with pytest.raises(
        ValueError, match="timestamp_start must not be after timestamp_end"
    ):
        async with aclosing(
            extractor.read_batches(
                timestamp_start=timestamp_start, timestamp_end=timestamp_end
            ),
        ) as batches:
            await anext(batches)

    consumer.consume.assert_not_called()


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
async def test_kafka_extractor_negative_non_utcoffset_timestamps(
    timestamp_start: datetime, timestamp_end: datetime
):
    """[unit] Даты без timezone не вызывают чтение Kafka."""
    consumer = Mock(spec=Consumer)
    settings = SimpleNamespace(
        music_kafka_topic_name="music-events",
        music_kafka_batch_size=4,
        music_kafka_timeout=1.0,
    )
    extractor = KafkaAnalyticsEventExtractor(consumer, settings)

    with pytest.raises(
        ValueError, match="timestamp_start and timestamp_end must be timezone-aware"
    ):
        async with aclosing(
            extractor.read_batches(
                timestamp_start=timestamp_start, timestamp_end=timestamp_end
            ),
        ) as batches:
            await anext(batches)

    consumer.consume.assert_not_called()
