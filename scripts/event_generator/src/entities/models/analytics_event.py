from datetime import datetime
from dataclasses import dataclass, field
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
    event_timestamp: datetime
    received_at: datetime

    user_id: UUID
    session_id: UUID
    track_id: UUID | None = None
    playback_id: UUID | None = None

    properties_schema_version: int = 1
    properties: dict = field(default_factory=dict)
