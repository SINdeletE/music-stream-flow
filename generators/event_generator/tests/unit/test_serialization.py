import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from generators.event_generator.src.entities.models import AnalyticsEvent, EventType
from generators.event_generator.src.serialization import event_to_json


@pytest.mark.parametrize("kind", list(EventType))
def test_serialization_positive_exact_contract(kind):
    """[unit] Kafka JSON contains exactly the consumer contract and normalizes timestamps to UTC."""
    now = datetime(2026, 10, 17, 15, tzinfo=timezone(timedelta(hours=3)))
    event = AnalyticsEvent(
        UUID(int=1), kind, UUID(int=2), UUID(int=3), UUID(int=4), UUID(int=5), now, now
    )
    actual = json.loads(event_to_json(event))
    assert actual == {
        "event_id": str(UUID(int=1)),
        "event_type": kind.value,
        "device_id": str(UUID(int=2)),
        "device_os_id": str(UUID(int=3)),
        "app_release_id": str(UUID(int=4)),
        "session_id": str(UUID(int=5)),
        "event_timestamp": now.astimezone(UTC).isoformat(),
        "received_at": now.astimezone(UTC).isoformat(),
        "user_id": None,
        "track_id": None,
        "playback_id": None,
    }
    event = replace(
        event, user_id=UUID(int=6), track_id=UUID(int=7), playback_id=UUID(int=8)
    )
    assert json.loads(event_to_json(event))["user_id"] == str(UUID(int=6))


@pytest.mark.parametrize("name", ["event_timestamp", "received_at"])
def test_serialization_negative_non_utcoffset_timestamp(name):
    """[unit] Timestamps without a timezone are rejected before Kafka enqueue."""
    now = datetime(2026, 10, 17, tzinfo=UTC)
    event = AnalyticsEvent(
        UUID(int=1),
        EventType.session_started,
        UUID(int=2),
        UUID(int=3),
        UUID(int=4),
        UUID(int=5),
        now,
        now,
    )
    with pytest.raises(ValueError):
        event_to_json(replace(event, **{name: now.replace(tzinfo=None)}))
