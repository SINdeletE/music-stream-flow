from datetime import timedelta
from threading import Event
from unittest.mock import Mock

import pytest

from generators.event_generator.src.runtime import EventGenerator
from generators.event_generator.src.source import SourceSnapshot


def make_generator(settings_factory, snapshot, clock, **overrides):
    source = Mock()
    source.read_snapshot.return_value = snapshot
    publisher = Mock()
    settings = settings_factory(**overrides)
    return EventGenerator(
        source, publisher, settings, clock=clock.monotonic, utcnow=clock.utcnow
    )


def test_event_generator_positive_quota_and_interval(settings_factory, snapshot, clock):
    """[unit] Start, selection and end share one interval and quota across periods."""
    generator = make_generator(
        settings_factory,
        snapshot,
        clock,
        max_per_period=1,
        period_ms=1000,
        interval_ms=100,
    )
    assert generator.step()
    session_id = generator.publisher.send.call_args.args[0].session_id
    clock.advance(0.5)
    assert not generator.step()
    clock.advance(0.5)
    assert generator.step()
    assert generator.publisher.send.call_args.args[0].session_id == session_id
    assert generator.sent_in_period == 1
    assert generator.total_sent == 2
    assert generator.publisher.send.call_count == 2


def test_event_generator_positive_round_robin_shared_quota(
    settings_factory, snapshot, clock
):
    """[unit] Concurrent sessions cannot each consume a separate copy of the global limit."""
    generator = make_generator(
        settings_factory,
        snapshot,
        clock,
        max_active_sessions=3,
        max_per_period=3,
        period_ms=1000,
        interval_ms=100,
    )
    for _ in range(3):
        assert generator.step()
        clock.advance(0.2)
    assert len(generator.engine.sessions) == 3
    assert not generator.step()
    assert generator.total_sent == 3


def test_event_generator_positive_spacing_after_ack(settings_factory, snapshot, clock):
    """[unit] Sending intervals begin after delivery confirmation, not before it."""
    generator = make_generator(settings_factory, snapshot, clock, interval_ms=500)
    generator.publisher.send.side_effect = lambda _: clock.advance(0.7)
    assert generator.step()
    assert generator.next_event_at == pytest.approx(1.2)
    clock.advance(0.49)
    assert not generator.step()
    clock.advance(0.02)
    assert generator.step()


def test_event_generator_positive_no_catchup_burst(settings_factory, snapshot, clock):
    """[unit] Delayed execution resets the current window without replaying missed quotas."""
    generator = make_generator(
        settings_factory, snapshot, clock, max_per_period=10, period_ms=1000
    )
    assert generator.step()
    clock.advance(20)
    assert generator.step()
    assert not generator.step()
    assert generator.period_start == 20
    assert generator.total_sent == 2
    assert generator.sent_in_period == 1


def test_event_generator_positive_zero_limit(settings_factory, snapshot, clock):
    """[unit] A zero limit generates no events and does not query PostgreSQL."""
    generator = make_generator(settings_factory, snapshot, clock, max_per_period=0)
    assert not generator.step()
    generator.source.read_snapshot.assert_not_called()
    generator.publisher.send.assert_not_called()


def test_event_generator_positive_missing_sources_refresh_without_spending(
    settings_factory, snapshot, clock
):
    """[unit] Empty sources wait for a refresh and consume neither quota nor synthetic sessions."""
    generator = make_generator(
        settings_factory, snapshot, clock, source_refresh_interval_ms=1000
    )
    generator.source.read_snapshot.side_effect = [SourceSnapshot(), snapshot]
    assert not generator.step()
    assert generator.sent_in_period == 0
    assert not generator.engine.sessions
    clock.advance(0.5)
    assert not generator.step()
    assert generator.source.read_snapshot.call_count == 1
    clock.advance(0.5)
    assert generator.step()
    assert generator.source.read_snapshot.call_count == 2


def test_event_generator_negative_delivery_does_not_advance(
    settings_factory, snapshot, clock
):
    """[unit] A failed delivery propagates without advancing the session or success counters."""
    generator = make_generator(settings_factory, snapshot, clock)
    generator.publisher.send.side_effect = TimeoutError("Synthetic delivery timeout")
    with pytest.raises(TimeoutError):
        generator.step()
    assert generator.total_sent == generator.sent_in_period == 0
    assert not generator.engine.sessions
    assert generator.engine.pending is not None


def test_event_generator_positive_shutdown_does_not_make_session_end(
    settings_factory, snapshot, clock
):
    """[unit] Shutdown leaves active sessions open without exceeding the quota for fake endings."""
    generator = make_generator(settings_factory, snapshot, clock)
    assert generator.step()
    stop = Event()
    stop.set()
    clock.advance(1)
    assert not generator.step(stop)
    assert len(generator.engine.sessions) == 1
    assert generator.publisher.send.call_count == 1


def test_event_generator_positive_stop_during_refresh(
    settings_factory, snapshot, clock
):
    """[unit] A shutdown arriving during a database query prevents subsequent sending."""
    generator = make_generator(settings_factory, snapshot, clock)
    stop = Event()

    def read(_):
        stop.set()
        return snapshot

    generator.source.read_snapshot.side_effect = read
    assert not generator.step(stop)
    generator.publisher.send.assert_not_called()


def test_event_generator_positive_utc_backwards_waits(
    settings_factory, snapshot, clock
):
    """[unit] A backward UTC clock jump cannot emit events before the last sent timestamp."""
    generator = make_generator(settings_factory, snapshot, clock)
    generator.step()
    clock.advance(1)
    generator.utcnow = lambda: clock.utcnow() - timedelta(seconds=2)
    assert not generator.step()
    assert generator.total_sent == 1


def test_event_generator_positive_run_stops_after_pending_delivery(
    settings_factory, snapshot, clock
):
    """[unit] A shutdown during delivery accepts that message and does not generate session_ended."""
    generator = make_generator(settings_factory, snapshot, clock)
    stop = Event()
    generator.publisher.send.side_effect = lambda _: stop.set()
    generator.run(stop)
    assert generator.total_sent == 1
    assert len(generator.engine.sessions) == 1
    assert generator.publisher.send.call_count == 1
