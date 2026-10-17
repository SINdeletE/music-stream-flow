from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from generators.event_generator.src import publisher as publisher_module
from generators.event_generator.src.entities.generates import SessionEngine
from generators.event_generator.src.publisher import KafkaPublisher


class FakeProducer:
    def __init__(self, clock, *, error=None, delivers=True, buffers=0):
        self.clock = clock
        self.error = error
        self.delivers = delivers
        self.buffers = buffers
        self.queued = []
        self.callback = None

    def produce(self, *args, **kwargs):
        if self.buffers:
            self.buffers -= 1
            raise BufferError("Synthetic queue full")
        self.queued.append((args, kwargs))
        self.callback = kwargs["on_delivery"]

    def poll(self, timeout):
        self.clock.advance(timeout)
        if self.callback is not None and self.delivers:
            callback, self.callback = self.callback, None
            callback(self.error, None)

    def flush(self, timeout):
        return 0


def event(settings_factory, snapshot, clock):
    return SessionEngine(settings_factory()).prepare(snapshot, clock.utcnow()).event


def test_publisher_positive_delivery_and_session_key(settings_factory, snapshot, clock):
    """[unit] Delivery is confirmed through poll and uses session_id as its partition key."""
    producer = FakeProducer(clock)
    publisher = KafkaPublisher(
        settings_factory(), producer=producer, clock=clock.monotonic
    )
    payload = event(settings_factory, snapshot, clock)
    publisher.send(payload)
    assert len(producer.queued) == 1
    args, kwargs = producer.queued[0]
    assert args == ("music-events",)
    assert kwargs["key"] == str(payload.session_id).encode()
    assert "partition" not in kwargs
    assert kwargs["timestamp"] == int(payload.event_timestamp.timestamp() * 1000)
    assert not publisher.failed
    assert not publisher.in_flight


def test_publisher_positive_idempotence_configuration(settings_factory, monkeypatch):
    """[unit] One producer enables idempotence and all acknowledgements."""
    factory = Mock()
    monkeypatch.setattr(publisher_module, "Producer", factory)
    KafkaPublisher(settings_factory())
    configuration = factory.call_args.args[0]
    assert configuration["enable.idempotence"] is True
    assert configuration["acks"] == "all"
    assert configuration["bootstrap.servers"] == "kafka:9092"


def test_publisher_positive_queue_backpressure(settings_factory, snapshot, clock):
    """[unit] Queue backpressure polls and retries the same event without creating another payload."""
    producer = FakeProducer(clock, buffers=2)
    publisher = KafkaPublisher(
        settings_factory(), producer=producer, clock=clock.monotonic
    )
    publisher.send(event(settings_factory, snapshot, clock))
    assert len(producer.queued) == 1


@pytest.mark.parametrize("failure", ["callback", "timeout"])
def test_publisher_negative_delivery_failure_is_terminal(
    settings_factory, snapshot, clock, failure
):
    """[unit] Failed or timed-out delivery is terminal and cannot be skipped for another event."""
    producer = FakeProducer(
        clock,
        error=KafkaError(KafkaError._MSG_TIMED_OUT),
        delivers=failure == "callback",
    )
    publisher = KafkaPublisher(
        settings_factory(delivery_timeout_ms=200),
        producer=producer,
        clock=clock.monotonic,
    )
    payload = event(settings_factory, snapshot, clock)
    with pytest.raises(KafkaException if failure == "callback" else TimeoutError):
        publisher.send(payload)
    assert publisher.failed
    with pytest.raises(RuntimeError):
        publisher.send(payload)
    assert len(producer.queued) == 1


def test_publisher_positive_metadata_checks_do_not_create_topic(
    settings_factory, clock
):
    """[unit] Startup lists all metadata rather than requesting auto-creation of a named topic."""
    producer = Mock()
    producer.list_topics.return_value = SimpleNamespace(
        brokers={1: object()},
        topics={"music-events": SimpleNamespace(error=None, partitions={0: object()})},
    )
    publisher = KafkaPublisher(
        settings_factory(), producer=producer, clock=clock.monotonic
    )
    assert publisher.wait_ready(Event())
    assert producer.list_topics.call_args.args == ()
    assert set(producer.list_topics.call_args.kwargs) == {"timeout"}


def test_publisher_negative_missing_topic_timeout(settings_factory, clock):
    """[unit] A missing topic reaches a bounded startup deadline without auto-creation."""
    producer = Mock()
    producer.list_topics.return_value = SimpleNamespace(
        brokers={1: object()}, topics={}
    )
    publisher = KafkaPublisher(
        settings_factory(startup_timeout_seconds=1),
        producer=producer,
        clock=clock.monotonic,
    )
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = clock.advance
    with pytest.raises(TimeoutError):
        publisher.wait_ready(stop)


def test_publisher_negative_incomplete_flush(settings_factory):
    """[unit] Shutdown cannot silently discard messages left in the producer queue."""
    producer = Mock()
    producer.flush.return_value = 1
    publisher = KafkaPublisher(settings_factory(), producer=producer)
    with pytest.raises(TimeoutError):
        publisher.close()
    producer.flush.assert_called_once_with(30)
