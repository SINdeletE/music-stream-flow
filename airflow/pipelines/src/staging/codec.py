import json
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from uuid import UUID

from sqlalchemy import inspect

from src.models.kafka import AnalyticsEvent
from src.models.kafka.analytics_event import EventType

from .registry import SOURCES


def _json_value(value):
    if isinstance(value, (UUID, datetime)):
        return value.isoformat() if isinstance(value, datetime) else str(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unsupported staging value: {type(value).__name__}")


def serialize_batch(source: str, batch: list) -> str:
    model = SOURCES[source].model
    rows = []
    for entity in batch:
        if not isinstance(entity, model):
            raise TypeError(f"Expected {model.__name__}, got {type(entity).__name__}")
        if model is AnalyticsEvent:
            rows.append(asdict(entity))
        else:
            rows.append(
                {
                    column.key: getattr(entity, column.key)
                    for column in inspect(model).columns
                }
            )
    return json.dumps(rows, default=_json_value, ensure_ascii=False, sort_keys=True)


def deserialize_batch(source: str, payload: str) -> list:
    model = SOURCES[source].model
    rows = json.loads(payload)
    if not isinstance(rows, list):
        raise TypeError("Staging payload must be a JSON array")
    result = []
    for row in rows:
        if not isinstance(row, dict):
            raise TypeError("Staging row must be a JSON object")
        if model is AnalyticsEvent:
            types = {
                **dict.fromkeys(
                    (
                        "event_id",
                        "device_id",
                        "device_os_id",
                        "app_release_id",
                        "session_id",
                        "user_id",
                        "track_id",
                        "playback_id",
                    ),
                    UUID,
                ),
                "event_timestamp": datetime,
                "received_at": datetime,
                "event_type": EventType,
            }
        else:
            types = {
                column.key: column.type.python_type for column in inspect(model).columns
            }
        if set(row) != set(types):
            raise ValueError("Staging row columns do not match the source model")
        for name, value_type in types.items():
            value = row[name]
            if value is not None:
                if value_type is UUID:
                    row[name] = UUID(value)
                elif value_type is datetime:
                    row[name] = datetime.fromisoformat(value)
                    if row[name].utcoffset() is None:
                        raise ValueError("Staging timestamps must be timezone-aware")
                elif value_type is EventType:
                    row[name] = EventType(value)
        result.append(model(**row))
    return result
