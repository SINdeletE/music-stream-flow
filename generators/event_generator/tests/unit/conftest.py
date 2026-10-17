from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from generators.event_generator.src.settings import EventSettings, SourceSettings
from generators.event_generator.src.source import SessionContext, SourceSnapshot


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def utcnow(self):
        return datetime(2026, 10, 17, 12, tzinfo=UTC) + timedelta(seconds=self.value)

    def advance(self, seconds):
        self.value += seconds


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def settings_factory():
    def make(**overrides):
        settings = EventSettings(
            source=SourceSettings(
                "localhost", 5432, "event_test", "event_test", "not-a-real-password"
            ),
            max_active_sessions=1,
            session_min_tracks=2,
            session_max_tracks=2,
        )
        return replace(settings, **overrides)

    return make


@pytest.fixture
def snapshot():
    return SourceSnapshot(
        contexts=tuple(
            SessionContext(
                UUID(int=100 + i),
                UUID(int=200 + i),
                UUID(int=300 + i),
                UUID(int=400 + i),
            )
            for i in range(3)
        ),
        track_ids=(UUID(int=1000), UUID(int=1001)),
    )
