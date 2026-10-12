import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import datetime
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.models.kafka import AnalyticsEvent
from src.models.kafka.analytics_event import EventType
from src.settings import KafkaSettings


class KafkaAnalyticsEventExtractor:
    def __init__(self, consumer: Consumer, settings: KafkaSettings) -> None:
        self.consumer = consumer
        self.batch_size = settings.music_kafka_batch_size
        self.timeout = settings.music_kafka_timeout

    async def read_batches(
        self,
        timestamp_start: datetime,
        timestamp_end: datetime,
    ) -> AsyncGenerator[tuple[list[AnalyticsEvent], int], None]:
        if timestamp_start.utcoffset() is None or timestamp_end.utcoffset() is None:
            raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
        if timestamp_start > timestamp_end:
            raise ValueError("timestamp_start must not be after timestamp_end")
        if timestamp_start == timestamp_end:
            return

        assignment = self.consumer.assignment()
        if not assignment:
            raise RuntimeError(
                "Consumer must have an assigned partition before reading"
            )
        partition = assignment[0]
        low_offset, end_offset = await asyncio.to_thread(
            self.consumer.get_watermark_offsets,
            partition,
            timeout=self.timeout,
            cached=False,
        )
        if low_offset == end_offset:
            return

        while True:
            positions = await asyncio.to_thread(self.consumer.position, [partition])
            position = positions[0]
            if position.error is not None:
                raise KafkaException(position.error)
            if position.offset >= end_offset:
                return

            messages = await asyncio.to_thread(
                self.consumer.consume,
                num_messages=self.batch_size,
                timeout=self.timeout,
            )
            if not messages:
                continue

            batch: list[AnalyticsEvent] = []
            next_offset: int | None = None

            for msg in messages:
                error = msg.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        if msg.offset() >= end_offset:
                            if next_offset is not None:
                                yield batch, next_offset
                            return
                        continue
                    raise KafkaException(error)

                if msg.offset() >= end_offset:
                    if next_offset is not None:
                        yield batch, next_offset
                    return

                payload = msg.value()
                if payload is None:
                    raise ValueError("Analytics event message must have a payload")
                data = json.loads(payload)
                if not isinstance(data, dict):
                    raise TypeError("Analytics event payload must be a JSON object")

                data["event_type"] = EventType(data["event_type"])
                for name in (
                    "event_id",
                    "device_id",
                    "device_os_id",
                    "app_release_id",
                    "session_id",
                ):
                    data[name] = UUID(data[name])
                for name in ("user_id", "track_id", "playback_id"):
                    if data.get(name) is not None:
                        data[name] = UUID(data[name])
                for name in ("event_timestamp", "received_at"):
                    data[name] = datetime.fromisoformat(data[name])
                    if data[name].utcoffset() is None:
                        raise ValueError(f"{name} must be timezone-aware")

                event = AnalyticsEvent(**data)
                if event.event_timestamp >= timestamp_end:
                    if next_offset is not None:
                        yield batch, next_offset
                    return

                batch.append(event)
                next_offset = msg.offset() + 1

            if next_offset is not None:
                yield batch, next_offset
                if next_offset >= end_offset:
                    return
