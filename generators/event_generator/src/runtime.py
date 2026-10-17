import logging
from collections.abc import Callable
from datetime import UTC, datetime
from threading import Event
from time import monotonic

from .entities.generates import SessionEngine
from .publisher import KafkaPublisher
from .settings import EventSettings
from .source import OltpSource, SourceSnapshot

logger = logging.getLogger(__name__)


class EventGenerator:
    def __init__(
        self,
        source: OltpSource,
        publisher: KafkaPublisher,
        settings: EventSettings,
        *,
        clock: Callable[[], float] = monotonic,
        utcnow: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.source = source
        self.publisher = publisher
        self.settings = settings
        self.clock = clock
        self.utcnow = utcnow
        self.engine = SessionEngine(settings)
        self.snapshot = SourceSnapshot()
        self.next_refresh_at = clock()
        self.next_event_at = clock()
        self.period_start: float | None = None
        self.sent_in_period = 0
        self.total_sent = 0
        self.last_event_timestamp: datetime | None = None

    def _renew_period(self, now: float) -> None:
        period = self.settings.period_ms / 1000
        if self.period_start is None:
            self.period_start = now
        elif now >= self.period_start + period:
            self.period_start += (
                max(1, int((now - self.period_start) / period)) * period
            )
        else:
            return
        self.sent_in_period = 0
        self.next_event_at = max(now, self.next_event_at)

    def step(self, stop: Event | None = None) -> bool:
        if (stop is not None and stop.is_set()) or self.settings.max_per_period == 0:
            return False
        now = self.clock()
        self._renew_period(now)
        if now >= self.next_refresh_at:
            self.snapshot = self.source.read_snapshot(self.utcnow())
            self.next_refresh_at = (
                self.clock() + self.settings.source_refresh_interval_ms / 1000
            )
        now = self.clock()
        self._renew_period(now)
        if (
            self.sent_in_period >= self.settings.max_per_period
            or now < self.next_event_at
        ):
            return False
        if stop is not None and stop.is_set():
            return False
        event_time = self.utcnow()
        if (
            self.last_event_timestamp is not None
            and event_time < self.last_event_timestamp
        ):
            self.next_event_at = now + self.settings.interval_ms / 1000
            return False
        prepared = self.engine.prepare(self.snapshot, event_time)
        if prepared is None:
            self.next_event_at = self.next_refresh_at
            return False
        self.publisher.send(prepared.event)
        self.engine.confirm(prepared)
        self.sent_in_period += 1
        self.total_sent += 1
        self.last_event_timestamp = prepared.event.event_timestamp
        self.next_event_at = self.clock() + self.settings.interval_ms / 1000
        logger.info(
            "delivered type=%s session_id=%s period_count=%s limit=%s",
            prepared.event.event_type.value,
            prepared.event.session_id,
            self.sent_in_period,
            self.settings.max_per_period,
        )
        return True

    def run(self, stop: Event) -> None:
        while not stop.is_set():
            self.step(stop)
            if stop.is_set():
                break
            now = self.clock()
            if self.settings.max_per_period == 0:
                delay = 1
            else:
                period_end = self.period_start + self.settings.period_ms / 1000
                deadlines = [self.next_refresh_at, period_end]
                if self.sent_in_period < self.settings.max_per_period:
                    deadlines.append(self.next_event_at)
                delay = max(0.001, min(deadlines) - now)
            stop.wait(delay)
