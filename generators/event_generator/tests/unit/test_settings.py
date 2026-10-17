import pytest

from generators.event_generator.src import settings as settings_module
from generators.event_generator.src.settings import EventSettings


def environment():
    return {
        "OLTP_DATABASE": "test",
        "OLTP_USERNAME": "test",
        "OLTP_PASSWORD": "not-a-real-password",
    }


def test_event_settings_positive_defaults_and_read_only_source():
    """[unit] Event configuration is independent of OLTP generation quotas."""
    settings = EventSettings.from_env(
        {**environment(), "TRACK_INSERT_MAX_PER_PERIOD": "invalid"}
    )
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.kafka_topic == "music-events"
    assert settings.max_per_period == 100
    assert settings.source.port == 5432
    assert (
        "default_transaction_read_only=on"
        in settings.source.connection_kwargs()["options"]
    )
    assert "not-a-real-password" not in repr(settings)


def test_event_settings_positive_environment_overrides_dotenv(monkeypatch):
    """[unit] Process environment overrides the shared dotenv file at runtime."""
    monkeypatch.setattr(
        settings_module,
        "dotenv_values",
        lambda _: {**environment(), "EVENT_MAX_PER_PERIOD": "7"},
    )
    monkeypatch.setattr(settings_module, "environ", {"EVENT_MAX_PER_PERIOD": "9"})
    assert EventSettings.from_env().max_per_period == 9


def test_event_settings_positive_zero_and_multiple_brokers():
    """[unit] Zero disables sending and bootstrap servers accept multiple host:port pairs."""
    settings = EventSettings.from_env(
        {
            **environment(),
            "EVENT_MAX_PER_PERIOD": "0",
            "EVENT_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092,[::1]:9093",
        }
    )
    assert settings.max_per_period == 0


@pytest.mark.parametrize(
    "key,value",
    [
        ("EVENT_MAX_PER_PERIOD", "-1"),
        ("EVENT_MAX_PER_PERIOD", "abc"),
        ("EVENT_PERIOD_MS", "0"),
        ("EVENT_INTERVAL_MS", "0"),
        ("EVENT_MAX_ACTIVE_SESSIONS", "0"),
        ("EVENT_USER_POOL_MAX_SIZE", "0"),
        ("EVENT_SESSION_MIN_TRACKS", "0"),
        ("EVENT_SESSION_MAX_TRACKS", "0"),
        ("EVENT_SESSION_MIN_TRACKS", "6"),
        ("EVENT_SOURCE_REFRESH_INTERVAL_MS", "-1"),
        ("EVENT_STARTUP_TIMEOUT_SECONDS", "0"),
        ("EVENT_DELIVERY_TIMEOUT_MS", "0"),
        ("EVENT_ANONYMOUS_SESSION_PERCENT", "101"),
        ("EVENT_NEW_USER_PERCENT", "-1"),
        ("EVENT_LIKE_PERCENT", "99"),
        ("EVENT_DISLIKE_PERCENT", "100"),
        ("EVENT_KAFKA_BOOTSTRAP_SERVERS", "http://kafka:9092"),
        ("EVENT_KAFKA_BOOTSTRAP_SERVERS", "https://kafka:9092"),
        ("EVENT_KAFKA_BOOTSTRAP_SERVERS", "kafka:0"),
        ("EVENT_KAFKA_BOOTSTRAP_SERVERS", "kafka:65536"),
        ("EVENT_KAFKA_BOOTSTRAP_SERVERS", ""),
        ("EVENT_KAFKA_TOPIC", ""),
        ("EVENT_KAFKA_TOPIC", ".."),
        ("EVENT_KAFKA_TOPIC", "invalid topic"),
        ("OLTP_PORT", "abc"),
        ("OLTP_PORT", "0"),
        ("OLTP_PASSWORD", ""),
    ],
)
def test_event_settings_negative_invalid_values(key, value):
    """[unit] Invalid configuration is rejected before clients are opened."""
    with pytest.raises(ValueError):
        EventSettings.from_env({**environment(), key: value})
