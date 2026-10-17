from collections.abc import Callable
from threading import Event
from time import monotonic

from confluent_kafka import KafkaException, Producer

from .entities.models import AnalyticsEvent
from .serialization import event_to_json
from .settings import EventSettings


class KafkaPublisher:
    def __init__(
        self,
        settings: EventSettings,
        *,
        producer: Producer | None = None,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self.settings = settings
        self.clock = clock
        self.failed = False
        self.in_flight = False
        self.producer = (
            producer
            if producer is not None
            else Producer(
                {
                    "bootstrap.servers": settings.kafka_bootstrap_servers,
                    "enable.idempotence": True,
                    "acks": "all",
                    "linger.ms": 0,
                    "delivery.timeout.ms": settings.delivery_timeout_ms,
                    "request.timeout.ms": min(
                        5000, max(1, settings.delivery_timeout_ms // 2)
                    ),
                }
            )
        )

    def wait_ready(self, stop: Event) -> bool:
        deadline = self.clock() + self.settings.startup_timeout_seconds
        while not stop.is_set():
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise TimeoutError("Kafka or the configured topic did not become ready")
            try:
                metadata = self.producer.list_topics(timeout=min(2, remaining))
            except KafkaException:
                metadata = None
            if metadata is not None and metadata.brokers:
                topic = metadata.topics.get(self.settings.kafka_topic)
                if topic is not None and topic.error is None and topic.partitions:
                    return True
            stop.wait(min(0.2, max(0, deadline - self.clock())))
        return False

    def send(self, event: AnalyticsEvent) -> None:
        if self.failed or self.in_flight:
            raise RuntimeError("Publisher is failed or already has a pending delivery")
        value = event_to_json(event)
        self.in_flight = True
        result = []

        def delivered(error, message):
            result.append(error)

        deadline = self.clock() + self.settings.delivery_timeout_ms / 1000
        queued = False
        try:
            while True:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise TimeoutError("Kafka event delivery deadline exceeded")
                if not queued:
                    try:
                        self.producer.produce(
                            self.settings.kafka_topic,
                            key=str(event.session_id).encode("utf-8"),
                            value=value,
                            timestamp=int(event.event_timestamp.timestamp() * 1000),
                            on_delivery=delivered,
                        )
                    except BufferError:
                        pass
                    else:
                        queued = True
                self.producer.poll(min(0.1, remaining))
                if result:
                    if result[0] is not None:
                        raise KafkaException(result[0])
                    return
        except (KafkaException, TimeoutError, RuntimeError, ValueError):
            self.failed = True
            raise
        finally:
            self.in_flight = False

    def close(self) -> None:
        remaining = self.producer.flush(self.settings.delivery_timeout_ms / 1000)
        if remaining:
            raise TimeoutError("Kafka shutdown left undelivered messages")
