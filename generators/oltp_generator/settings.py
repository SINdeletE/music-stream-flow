from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from os import environ as process_environment
from pathlib import Path
from typing import Self

from dotenv import dotenv_values

ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
INSERT_DEFAULTS = {
    "device": 10,
    "genre": 20,
    "artist": 10,
    "app_release": 2,
    "release": 10,
    "installation": 20,
    "device_os_history": 10,
    "track": 100,
}
UPDATE_TABLES = ("installation", "artist", "release", "track")


@dataclass(frozen=True)
class FlowSettings:
    table: str
    operation: str
    period_ms: int
    interval_ms: int
    max_per_period: int = 0
    percent_per_period: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        if self.operation not in ("insert", "update"):
            raise ValueError("Unknown flow operation")
        allowed = INSERT_DEFAULTS if self.operation == "insert" else UPDATE_TABLES
        if self.table not in allowed:
            raise ValueError("Unsupported flow table")
        if self.period_ms <= 0 or self.interval_ms <= 0:
            raise ValueError("Periods and intervals must be positive")
        if self.max_per_period < 0:
            raise ValueError("Insert limits must be non-negative")
        if (
            not self.percent_per_period.is_finite()
            or not 0 <= self.percent_per_period <= 100
        ):
            raise ValueError("Update percentage must be between 0 and 100")

    @property
    def enabled(self) -> bool:
        return (
            self.max_per_period > 0
            if self.operation == "insert"
            else self.percent_per_period > 0
        )


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)
    flows: tuple[FlowSettings, ...] = ()
    random_seed: int = 42
    startup_timeout_seconds: int = 120

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
        if self.startup_timeout_seconds <= 0:
            raise ValueError("OLTP_STARTUP_TIMEOUT_SECONDS must be positive")
        keys = [(flow.table, flow.operation) for flow in self.flows]
        if len(keys) != len(set(keys)):
            raise ValueError("Flow definitions must be unique")

    def connection_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "connect_timeout": 3,
            "application_name": "music_oltp_generator",
        }

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> Self:
        values = (
            {**dotenv_values(ENV_FILE), **process_environment}
            if environment is None
            else environment
        )

        def integer(key: str, default: int) -> int:
            try:
                return int(values.get(key, str(default)))
            except (TypeError, ValueError) as error:
                raise ValueError(f"{key} must be an integer") from error

        flows = []
        for table, default in INSERT_DEFAULTS.items():
            prefix = f"{table.upper()}_INSERT"
            flows.append(
                FlowSettings(
                    table=table,
                    operation="insert",
                    max_per_period=integer(f"{prefix}_MAX_PER_PERIOD", default),
                    period_ms=integer(f"{prefix}_PERIOD_MS", 60000),
                    interval_ms=integer(f"{prefix}_INTERVAL_MS", 100),
                )
            )
        for table in UPDATE_TABLES:
            prefix = f"{table.upper()}_UPDATE"
            try:
                percent = Decimal(values.get(f"{prefix}_PERCENT_PER_PERIOD", "10"))
            except (InvalidOperation, TypeError) as error:
                raise ValueError(
                    f"{prefix}_PERCENT_PER_PERIOD must be a percentage"
                ) from error
            flows.append(
                FlowSettings(
                    table=table,
                    operation="update",
                    percent_per_period=percent,
                    period_ms=integer(f"{prefix}_PERIOD_MS", 60000),
                    interval_ms=integer(f"{prefix}_INTERVAL_MS", 500),
                )
            )
        return cls(
            host=values.get("OLTP_HOST", "music-oltp"),
            port=integer("OLTP_PORT", 5432),
            database=values.get("OLTP_DATABASE", "") or "",
            username=values.get("OLTP_USERNAME", "") or "",
            password=values.get("OLTP_PASSWORD", "") or "",
            random_seed=integer("OLTP_RANDOM_SEED", 42),
            startup_timeout_seconds=integer("OLTP_STARTUP_TIMEOUT_SECONDS", 120),
            flows=tuple(flows),
        )
