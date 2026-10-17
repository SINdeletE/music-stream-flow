from decimal import Decimal

import pytest

from generators.oltp_generator.settings import FlowSettings, Settings


def environment():
    return {
        "OLTP_DATABASE": "music_test",
        "OLTP_USERNAME": "test",
        "OLTP_PASSWORD": "not-a-real-password",
    }


def test_settings_positive_defaults():
    """[unit] Settings expose independent defaults without reading project env files."""
    settings = Settings.from_env(environment())
    assert settings.host == "music-oltp"
    assert settings.port == 5432
    assert len(settings.flows) == 12
    assert sum(flow.operation == "update" for flow in settings.flows) == 4
    assert "not-a-real-password" not in repr(settings)
    assert settings.connection_kwargs()["password"] == "not-a-real-password"


def test_settings_positive_independent_overrides_and_disabled_flows():
    """[unit] Every flow accepts its own timing and zero disables only that flow."""
    settings = Settings.from_env(
        {
            **environment(),
            "TRACK_INSERT_MAX_PER_PERIOD": "0",
            "ARTIST_INSERT_PERIOD_MS": "3000",
            "ARTIST_INSERT_INTERVAL_MS": "125",
            "TRACK_UPDATE_PERCENT_PER_PERIOD": "12.5",
        }
    )
    flows = {(flow.table, flow.operation): flow for flow in settings.flows}
    assert not flows["track", "insert"].enabled
    assert flows["artist", "insert"].period_ms == 3000
    assert flows["artist", "insert"].interval_ms == 125
    assert flows["track", "update"].percent_per_period == Decimal("12.5")


@pytest.mark.parametrize(
    "key,value",
    [
        ("OLTP_PORT", "0"),
        ("OLTP_PORT", "65536"),
        ("OLTP_PORT", "abc"),
        ("OLTP_PASSWORD", ""),
        ("OLTP_STARTUP_TIMEOUT_SECONDS", "0"),
        ("TRACK_INSERT_MAX_PER_PERIOD", "-1"),
        ("TRACK_INSERT_PERIOD_MS", "0"),
        ("TRACK_INSERT_INTERVAL_MS", "-1"),
        ("TRACK_UPDATE_PERCENT_PER_PERIOD", "101"),
        ("TRACK_UPDATE_PERCENT_PER_PERIOD", "-1"),
        ("TRACK_UPDATE_PERCENT_PER_PERIOD", "NaN"),
        ("TRACK_UPDATE_PERCENT_PER_PERIOD", "Infinity"),
        ("TRACK_UPDATE_PERCENT_PER_PERIOD", "abc"),
    ],
)
def test_settings_negative_invalid_values(key, value):
    """[unit] Invalid settings fail before opening a database connection."""
    with pytest.raises(ValueError):
        Settings.from_env({**environment(), key: value})


@pytest.mark.parametrize(
    "table", ["device", "genre", "app_release", "device_os_history", "track_genre"]
)
def test_settings_negative_unsupported_update(table):
    """[unit] Finite catalogs, immutable entities and junctions have no update flow."""
    with pytest.raises(ValueError):
        FlowSettings(table, "update", 1000, 100, percent_per_period=Decimal(10))
