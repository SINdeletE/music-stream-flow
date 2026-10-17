from unittest.mock import Mock

import psycopg
import pytest

from generators.event_generator.src import main as entrypoint


def setup_main(monkeypatch, settings_factory):
    connection, publisher, runtime = Mock(), Mock(), Mock()
    publisher.wait_ready.return_value = True
    runtime.total_sent = 3
    runtime.engine.sessions = [object()]
    runtime.run.side_effect = lambda stop: stop.set()
    monkeypatch.setattr(
        entrypoint.EventSettings, "from_env", lambda: settings_factory()
    )
    monkeypatch.setattr(entrypoint.signal, "signal", Mock())
    connect = Mock(return_value=connection)
    factory = Mock(return_value=publisher)
    runtime_factory = Mock(return_value=runtime)
    monkeypatch.setattr(entrypoint, "connect", connect)
    monkeypatch.setattr(entrypoint, "KafkaPublisher", factory)
    monkeypatch.setattr(entrypoint, "EventGenerator", runtime_factory)
    return connection, publisher, runtime, connect, factory, runtime_factory


def test_event_main_positive_shutdown_closes_resources(monkeypatch, settings_factory):
    """[unit] Normal shutdown flushes Kafka and closes the PostgreSQL connection."""
    connection, publisher, runtime, *_ = setup_main(monkeypatch, settings_factory)
    assert entrypoint.main() == 0
    publisher.close.assert_called_once()
    connection.close.assert_called_once()
    assert len(runtime.engine.sessions) == 1


@pytest.mark.parametrize("failure", ["runtime", "startup", "flush"])
def test_event_main_negative_errors_still_close_resources(
    monkeypatch, settings_factory, failure
):
    """[unit] Startup, generation and flush failures return nonzero and release resources."""
    connection, publisher, runtime, *_ = setup_main(monkeypatch, settings_factory)
    if failure == "runtime":
        runtime.run.side_effect = RuntimeError("Synthetic runtime failure")
    elif failure == "startup":
        publisher.wait_ready.side_effect = TimeoutError("Synthetic startup timeout")
    else:
        publisher.close.side_effect = TimeoutError("Synthetic flush timeout")
    assert entrypoint.main() == 1
    publisher.close.assert_called_once()
    connection.close.assert_called_once()


def test_event_main_negative_validation_precedes_connections(
    monkeypatch, settings_factory
):
    """[unit] Invalid environment configuration opens neither PostgreSQL nor Kafka."""
    *_, connect, factory, _ = setup_main(monkeypatch, settings_factory)

    def invalid():
        raise ValueError("Invalid settings")

    monkeypatch.setattr(entrypoint.EventSettings, "from_env", invalid)
    assert entrypoint.main() == 1
    connect.assert_not_called()
    factory.assert_not_called()


def test_event_connect_negative_bounded_readiness(monkeypatch, settings_factory, clock):
    """[unit] PostgreSQL readiness is bounded and never silently skips connection failure."""
    monkeypatch.setattr(entrypoint, "monotonic", clock.monotonic)
    monkeypatch.setattr(
        entrypoint.psycopg, "connect", Mock(side_effect=psycopg.OperationalError())
    )
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = clock.advance
    with pytest.raises(TimeoutError):
        entrypoint.connect(settings_factory(startup_timeout_seconds=1), stop)
    assert clock.value == 1
