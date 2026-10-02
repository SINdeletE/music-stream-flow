from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class FctEvent(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.fct_events"
    distributed_table_name: ClassVar[str | None] = "music_dwh.fct_events_all"

    event_sk: int
    event_id: UUID
    event_type_sk: int
    device_sk: int
    device_os_sk: int
    app_release_sk: int
    track_sk: int
    session_id: UUID
    playback_id: UUID | None = None
    event_timestamp: datetime
    received_at: datetime
    loaded_at: datetime
    updated_at: datetime
