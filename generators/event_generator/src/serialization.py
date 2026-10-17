import json
from dataclasses import asdict
from datetime import UTC

from .entities.models import AnalyticsEvent


def event_to_json(event: AnalyticsEvent) -> bytes:
    values = asdict(event)
    values["event_type"] = event.event_type.value
    for name in (
        "event_id",
        "device_id",
        "device_os_id",
        "app_release_id",
        "session_id",
        "user_id",
        "track_id",
        "playback_id",
    ):
        value = values[name]
        values[name] = str(value) if value is not None else None
    for name in ("event_timestamp", "received_at"):
        value = values[name]
        if value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        values[name] = value.astimezone(UTC).isoformat()
    return json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
