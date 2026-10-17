import re
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from os import environ
from pathlib import Path
from typing import Self

from dotenv import dotenv_values

ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


@dataclass(frozen=True)
class SourceSettings:
    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        if not all(
            (
                self.host.strip(),
                self.database.strip(),
                self.username.strip(),
                self.password,
            )
        ):
            raise ValueError("OLTP connection settings must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("OLTP_PORT must be between 1 and 65535")

    def connection_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "connect_timeout": 3,
            "application_name": "music_event_generator",
            "options": "-c default_transaction_read_only=on -c statement_timeout=10000",
        }


@dataclass(frozen=True)
class EventSettings:
    source: SourceSettings
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "music-events"
    max_per_period: int = 100
    period_ms: int = 60000
    interval_ms: int = 500
    max_active_sessions: int = 10
    session_min_tracks: int = 1
    session_max_tracks: int = 5
    anonymous_session_percent: int = 20
    new_user_percent: int = 25
    user_pool_max_size: int = 1000
    like_percent: int = 30
    dislike_percent: int = 10
    source_refresh_interval_ms: int = 5000
    startup_timeout_seconds: int = 120
    delivery_timeout_ms: int = 30000
    random_seed: int = 42

    def __post_init__(self) -> None:
        if self.max_per_period < 0:
            raise ValueError("EVENT_MAX_PER_PERIOD must be non-negative")
        for name in (
            "period_ms",
            "interval_ms",
            "max_active_sessions",
            "session_min_tracks",
            "session_max_tracks",
            "user_pool_max_size",
            "source_refresh_interval_ms",
            "startup_timeout_seconds",
            "delivery_timeout_ms",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"EVENT_{name.upper()} must be positive")
        if self.session_min_tracks > self.session_max_tracks:
            raise ValueError(
                "EVENT_SESSION_MIN_TRACKS must not exceed EVENT_SESSION_MAX_TRACKS"
            )
        for name in (
            "anonymous_session_percent",
            "new_user_percent",
            "like_percent",
            "dislike_percent",
        ):
            if not 0 <= getattr(self, name) <= 100:
                raise ValueError(f"EVENT_{name.upper()} must be between 0 and 100")
        if self.like_percent + self.dislike_percent > 100:
            raise ValueError("Reaction percentages must not exceed 100 in total")
        if not re.fullmatch(
            r"[A-Za-z0-9_.-]{1,249}", self.kafka_topic
        ) or self.kafka_topic in (".", ".."):
            raise ValueError("Invalid EVENT_KAFKA_TOPIC")
        for address in self.kafka_bootstrap_servers.split(","):
            match = re.fullmatch(r"(?:\[[^\]\s]+\]|[^:\s,/]+):(\d+)", address.strip())
            if match is None or not 1 <= int(match[1]) <= 65535:
                raise ValueError(
                    "EVENT_KAFKA_BOOTSTRAP_SERVERS must contain host:port addresses, not URLs"
                )

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> Self:
        values = (
            {**dotenv_values(ENV_FILE), **environ}
            if environment is None
            else environment
        )

        def integer(key: str, default: int) -> int:
            try:
                return int(values.get(key, str(default)))
            except (TypeError, ValueError) as error:
                raise ValueError(f"{key} must be an integer") from error

        source = SourceSettings(
            host=values.get("OLTP_HOST", "music-oltp") or "",
            port=integer("OLTP_PORT", 5432),
            database=values.get("OLTP_DATABASE", "") or "",
            username=values.get("OLTP_USERNAME", "") or "",
            password=values.get("OLTP_PASSWORD", "") or "",
        )
        arguments = {"source": source}
        for item in fields(cls):
            if item.name == "source":
                continue
            key = f"EVENT_{item.name.upper()}"
            arguments[item.name] = (
                values.get(key, item.default) or ""
                if item.name in ("kafka_bootstrap_servers", "kafka_topic")
                else integer(key, item.default)
            )
        return cls(**arguments)
