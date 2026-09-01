from datetime import datetime
from dataclasses import dataclass, field
from uuid import UUID
from enum import Enum

class EventType(Enum):
    session_started         = 'session_started'
    session_ended           = 'session_ended'
    track_started           = 'track_started'
    track_ended             = 'track_ended'
    track_paused            = 'track_paused'
    track_resumed           = 'track_resumed'
    track_skipped           = 'track_skipped'
    track_seeked            = 'track_seeked'
    track_completed         = 'track_completed'
    track_liked             = 'track_liked'
    track_disliked          = 'track_disliked'
    track_like_removed      = 'track_like_removed'
    track_added_to_playlist = 'track_added_to_playlist'

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
