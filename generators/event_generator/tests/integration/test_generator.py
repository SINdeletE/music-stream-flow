import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from threading import Event
from time import monotonic
from uuid import UUID, uuid4

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from generators.event_generator.src.publisher import KafkaPublisher
from generators.event_generator.src.runtime import EventGenerator
from generators.event_generator.src.settings import EventSettings
from generators.event_generator.src.source import OltpSource

from ._data import database_state, populate


@pytest.mark.parametrize("anonymous", [0, 100], ids=["authenticated", "anonymous"])
def test_event_generator_positive_real_kafka_sessions(
    database, reader, kafka_topic, anonymous
):
    """[integration] PostgreSQL-backed concurrent sessions reach Kafka in order and preserve real IDs."""
    source_settings, writer = database
    contexts, track_ids = populate(writer)
    before = database_state(writer)
    bootstrap, topic = kafka_topic
    settings = EventSettings(
        source=source_settings,
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=topic,
        max_per_period=1000,
        interval_ms=1,
        max_active_sessions=3,
        session_min_tracks=2,
        session_max_tracks=2,
        anonymous_session_percent=anonymous,
        like_percent=100,
        dislike_percent=0,
        source_refresh_interval_ms=1000,
    )
    publisher = KafkaPublisher(settings)
    elapsed = [0.0]
    base_time = datetime.now(UTC)
    runtime = EventGenerator(
        OltpSource(reader),
        publisher,
        settings,
        clock=lambda: elapsed[0],
        utcnow=lambda: base_time + timedelta(seconds=elapsed[0]),
    )
    sent = []
    first_sessions = set()
    completed = set()
    original_send = publisher.send

    def record(event):
        original_send(event)
        sent.append(event)
        if len(first_sessions) < 3:
            first_sessions.add(event.session_id)
        if event.event_type.value == "session_ended":
            completed.add(event.session_id)

    publisher.send = record
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"event-test-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    try:
        assert publisher.wait_ready(Event())
        for _ in range(100):
            runtime.step()
            elapsed[0] += 0.01
            if len(first_sessions) == 3 and first_sessions <= completed:
                break
        assert first_sessions <= completed
        assert runtime.total_sent == len(sent)
        assert len(sent) < 100
        consumer.assign(
            [TopicPartition(topic, index, OFFSET_BEGINNING) for index in range(3)]
        )
        messages = []
        deadline = monotonic() + 30
        while len(messages) < len(sent) and monotonic() < deadline:
            for message in consumer.consume(num_messages=100, timeout=0.2):
                assert message.error() is None
                messages.append(message)
        assert len(messages) == len(sent)
        by_session = defaultdict(list)
        valid_contexts = {
            (
                str(context["device_id"]),
                str(context["device_os_id"]),
                str(context["app_release_id"]),
            )
            for context in contexts
        }
        for message in messages:
            payload = json.loads(message.value())
            assert set(payload) == {
                "event_id",
                "event_type",
                "device_id",
                "device_os_id",
                "app_release_id",
                "session_id",
                "event_timestamp",
                "received_at",
                "user_id",
                "track_id",
                "playback_id",
            }
            UUID(payload["event_id"])
            assert message.key().decode() == payload["session_id"]
            assert (
                payload["device_id"],
                payload["device_os_id"],
                payload["app_release_id"],
            ) in valid_contexts
            assert payload["received_at"] == payload["event_timestamp"]
            assert datetime.fromisoformat(
                payload["event_timestamp"]
            ).utcoffset() == timedelta(0)
            if payload["track_id"] is not None:
                assert UUID(payload["track_id"]) in track_ids
            assert (payload["user_id"] is None) == (anonymous == 100)
            by_session[payload["session_id"]].append((message.partition(), payload))
        for session_id, rows in by_session.items():
            assert len({partition for partition, _ in rows}) == 1
            payloads = [payload for _, payload in rows]
            assert payloads[0]["event_type"] == "session_started"
            assert len({payload["user_id"] for payload in payloads}) == 1
            timestamps = [
                datetime.fromisoformat(payload["event_timestamp"])
                for payload in payloads
            ]
            assert timestamps == sorted(timestamps)
            current = None
            for payload in payloads:
                if payload["event_type"] in ("session_started", "session_ended"):
                    assert payload["track_id"] is payload["playback_id"] is None
                elif payload["event_type"] == "track_selected":
                    current = payload
                else:
                    assert anonymous == 0
                    assert payload["event_type"] == "track_liked"
                    assert (payload["track_id"], payload["playback_id"]) == (
                        current["track_id"],
                        current["playback_id"],
                    )
            if UUID(session_id) in first_sessions:
                expected = ["session_started"]
                for _ in range(2):
                    expected += ["track_selected"] + (
                        ["track_liked"] if anonymous == 0 else []
                    )
                expected.append("session_ended")
                assert [payload["event_type"] for payload in payloads] == expected
        assert database_state(writer) == before
    finally:
        consumer.close()
        publisher.close()


def test_event_generator_negative_missing_topic_is_not_created(database, kafka_service):
    """[integration] Startup timeout does not create a missing topic in Kafka."""
    source_settings, _ = database
    bootstrap, admin = kafka_service
    topic = f"not-created-{uuid4().hex}"
    settings = EventSettings(
        source=source_settings,
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=topic,
        startup_timeout_seconds=1,
    )
    publisher = KafkaPublisher(settings)
    try:
        with pytest.raises(TimeoutError):
            publisher.wait_ready(Event())
        assert topic not in admin.list_topics(timeout=5).topics
    finally:
        publisher.close()
