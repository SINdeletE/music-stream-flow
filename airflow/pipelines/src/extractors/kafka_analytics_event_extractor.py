import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from datetime import datetime
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.models.kafka import AnalyticsEvent
from src.models.kafka.analytics_event import EventType
from src.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaAnalyticsEventExtractor:
    def __init__(
        self,
        consumer: Consumer,
        settings: KafkaSettings,
        *,
        batch_size: int | None = None,
    ) -> None:
        self.consumer = consumer
        self.batch_size = (
            settings.music_kafka_batch_size if batch_size is None else batch_size
        )
        if self.batch_size < 1:
            raise ValueError("batch_size must be positive")
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
            logger.info(
                "Kafka reading skipped: empty interval timestamp_start=%s timestamp_end=%s",
                timestamp_start,
                timestamp_end,
            )
            return

        logger.info(
            "Kafka reading started: timestamp_start=%s timestamp_end=%s batch_size=%s timeout=%s",
            timestamp_start,
            timestamp_end,
            self.batch_size,
            self.timeout,
        )
        assignment = self.consumer.assignment()
        if not assignment:
            logger.error("Kafka reading failed: no assigned partition")
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
        logger.info(
            "Kafka offset boundaries: topic=%s partition=%s low_offset=%s end_offset=%s",
            partition.topic,
            partition.partition,
            low_offset,
            end_offset,
        )
        if low_offset == end_offset:
            logger.info(
                "Kafka reading finished: empty partition topic=%s partition=%s end_offset=%s",
                partition.topic,
                partition.partition,
                end_offset,
            )
            return

        empty_polls = 0
        while True:
            positions = await asyncio.to_thread(self.consumer.position, [partition])
            position = positions[0]
            if position.error is not None:
                logger.error(
                    "Kafka position failed: topic=%s partition=%s error=%s",
                    partition.topic,
                    partition.partition,
                    position.error,
                )
                raise KafkaException(position.error)
            logger.debug(
                "Kafka position: topic=%s partition=%s position=%s end_offset=%s",
                partition.topic,
                partition.partition,
                position.offset,
                end_offset,
            )
            if position.offset >= end_offset:
                logger.info(
                    "Kafka reading finished: position reached end_offset topic=%s partition=%s position=%s end_offset=%s",
                    partition.topic,
                    partition.partition,
                    position.offset,
                    end_offset,
                )
                return

            messages = await asyncio.to_thread(
                self.consumer.consume,
                num_messages=self.batch_size,
                timeout=self.timeout,
            )
            if not messages:
                empty_polls += 1
                log = logger.warning if empty_polls == 1 else logger.debug
                log(
                    "Kafka poll returned no messages: topic=%s partition=%s position_before_poll=%s end_offset=%s timeout=%s empty_polls=%s; retrying",
                    partition.topic,
                    partition.partition,
                    position.offset,
                    end_offset,
                    self.timeout,
                    empty_polls,
                )
                continue
            if empty_polls:
                logger.info(
                    "Kafka messages received after empty polls: topic=%s partition=%s empty_polls=%s messages=%s",
                    partition.topic,
                    partition.partition,
                    empty_polls,
                    len(messages),
                )
                empty_polls = 0
            logger.debug(
                "Kafka poll received messages: topic=%s partition=%s messages=%s",
                partition.topic,
                partition.partition,
                len(messages),
            )

            batch: list[AnalyticsEvent] = []
            next_offset: int | None = None

            for msg in messages:
                error = msg.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        if msg.offset() >= end_offset:
                            logger.info(
                                "Kafka reading finished: partition EOF topic=%s partition=%s offset=%s end_offset=%s batch_rows=%s next_offset=%s",
                                partition.topic,
                                partition.partition,
                                msg.offset(),
                                end_offset,
                                len(batch),
                                next_offset,
                            )
                            if next_offset is not None:
                                yield batch, next_offset
                            return
                        logger.debug(
                            "Kafka partition EOF before end_offset: topic=%s partition=%s offset=%s end_offset=%s",
                            partition.topic,
                            partition.partition,
                            msg.offset(),
                            end_offset,
                        )
                        continue
                    logger.error(
                        "Kafka message failed: topic=%s partition=%s offset=%s error=%s",
                        partition.topic,
                        partition.partition,
                        msg.offset(),
                        error,
                    )
                    raise KafkaException(error)

                if msg.offset() >= end_offset:
                    logger.info(
                        "Kafka reading finished: message reached end_offset topic=%s partition=%s offset=%s end_offset=%s batch_rows=%s next_offset=%s",
                        partition.topic,
                        partition.partition,
                        msg.offset(),
                        end_offset,
                        len(batch),
                        next_offset,
                    )
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
                    logger.info(
                        "Kafka reading finished: timestamp boundary reached topic=%s partition=%s offset=%s event_timestamp=%s timestamp_end=%s batch_rows=%s next_offset=%s",
                        partition.topic,
                        partition.partition,
                        msg.offset(),
                        event.event_timestamp,
                        timestamp_end,
                        len(batch),
                        next_offset,
                    )
                    if next_offset is not None:
                        yield batch, next_offset
                    return

                batch.append(event)
                next_offset = msg.offset() + 1

            if next_offset is not None:
                logger.info(
                    "Kafka batch ready: topic=%s partition=%s rows=%s next_offset=%s end_offset=%s",
                    partition.topic,
                    partition.partition,
                    len(batch),
                    next_offset,
                    end_offset,
                )
                yield batch, next_offset
                if next_offset >= end_offset:
                    logger.info(
                        "Kafka reading finished: batch reached end_offset topic=%s partition=%s next_offset=%s end_offset=%s",
                        partition.topic,
                        partition.partition,
                        next_offset,
                        end_offset,
                    )
                    return
