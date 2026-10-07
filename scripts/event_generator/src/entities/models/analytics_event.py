from datetime import datetime
from dataclasses import dataclass
from uuid import UUID
from enum import Enum

class EventType(Enum):
    session_started         = 'session_started'
    session_ended           = 'session_ended'
    track_selected          = 'track_selected'
    track_liked             = 'track_liked'
    track_disliked          = 'track_disliked'

@dataclass
class AnalyticsEvent:
    event_id: UUID
    event_type: EventType
    device_id: UUID
    device_os_id: UUID
    app_release_id: UUID
    session_id: UUID

    event_timestamp: datetime
    received_at: datetime
    user_id: UUID | None = None
    track_id: UUID | None = None
    playback_id: UUID | None = None
