from datetime import datetime
from typing import Protocol
from uuid import UUID

from src.models.dwh import FctEvent


class AnalyticsEventSource(Protocol):
    event_id: UUID
    user_id: UUID | None
    session_id: UUID
    playback_id: UUID | None
    event_timestamp: datetime
    received_at: datetime


class FctEventMapper:
    @staticmethod
    def to_dwh(
        entity: AnalyticsEventSource,
        load_id: UUID,
        *,
        event_type_sk: int,
        device_sk: int,
        device_os_sk: int,
        app_release_sk: int,
        track_sk: int | None,
    ) -> FctEvent:
        for name, value in (
            ("event_type_sk", event_type_sk),
            ("device_sk", device_sk),
            ("device_os_sk", device_os_sk),
            ("app_release_sk", app_release_sk),
        ):
            if value is None:
                raise ValueError(f"{name} must be resolved before mapping a fact")

        return FctEvent(
            event_id=entity.event_id,
            user_id=entity.user_id,
            event_type_sk=event_type_sk,
            device_sk=device_sk,
            device_os_sk=device_os_sk,
            app_release_sk=app_release_sk,
            track_sk=track_sk,
            session_id=entity.session_id,
            playback_id=entity.playback_id,
            event_timestamp=entity.event_timestamp,
            received_at=entity.received_at,
            load_id=load_id,
        )
