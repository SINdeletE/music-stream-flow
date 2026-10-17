from threading import Event
from unittest.mock import Mock

import psycopg
import pytest

from generators.oltp_generator import __main__ as entrypoint


def test_main_negative_invalid_config_does_not_connect(monkeypatch):
    """[unit] Configuration errors terminate before attempting a database connection."""

    def invalid():
        raise ValueError("Invalid configuration")

    connect = Mock()
    monkeypatch.setattr(entrypoint.Settings, "from_env", invalid)
    monkeypatch.setattr(entrypoint, "connect", connect)
    monkeypatch.setattr(entrypoint.signal, "signal", Mock())
    assert entrypoint.main() == 1
    connect.assert_not_called()


def test_connect_positive_retry_then_ready(settings_factory, clock, monkeypatch):
    """[unit] Startup retries a temporarily unavailable database and returns its connection."""
    connection = Mock()
    connector = Mock(side_effect=[psycopg.OperationalError(), connection])
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = clock.advance
    monkeypatch.setattr(entrypoint, "monotonic", clock.monotonic)
    monkeypatch.setattr(entrypoint.psycopg, "connect", connector)
    assert entrypoint.connect(settings_factory(), stop) is connection
    assert connector.call_count == 2
    assert connector.call_args.kwargs["autocommit"] is True


def test_connect_negative_startup_deadline(settings_factory, clock, monkeypatch):
    """[unit] Database startup retries stop at the configured readiness deadline."""
    monkeypatch.setattr(entrypoint, "monotonic", clock.monotonic)
    monkeypatch.setattr(
        entrypoint.psycopg, "connect", Mock(side_effect=psycopg.OperationalError())
    )
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = clock.advance
    with pytest.raises(TimeoutError):
        entrypoint.connect(settings_factory(), stop)
    assert clock.value == 120


def test_connect_positive_shutdown_before_connect(settings_factory, monkeypatch):
    """[unit] A shutdown signal cancels startup without opening a connection."""
    connector = Mock()
    monkeypatch.setattr(entrypoint.psycopg, "connect", connector)
    stop = Event()
    stop.set()
    assert entrypoint.connect(settings_factory(), stop) is None
    connector.assert_not_called()
