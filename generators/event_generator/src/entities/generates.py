from collections import deque
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from random import Random
from uuid import UUID, uuid4, uuid7

from ..settings import EventSettings
from ..source import SessionContext, SourceSnapshot
from .models import AnalyticsEvent, EventType


class UserPool:
    def __init__(self, settings: EventSettings, random: Random) -> None:
        self.settings = settings
        self.random = random
        self.users: list[UUID] = []

    def choose(self) -> UUID | None:
        if self.random.randrange(100) < self.settings.anonymous_session_percent:
            return None
        if not self.users or (
            len(self.users) < self.settings.user_pool_max_size
            and self.random.randrange(100) < self.settings.new_user_percent
        ):
            user_id = uuid4()
            self.users.append(user_id)
            return user_id
        return self.random.choice(self.users)


@dataclass(frozen=True)
class Session:
    context: SessionContext
    session_id: UUID
    user_id: UUID | None
    target_tracks: int
    started: bool = False
    selected_tracks: int = 0
    track_id: UUID | None = None
    playback_id: UUID | None = None
    reaction: EventType | None = None
    ended: bool = False


@dataclass(frozen=True)
class PreparedEvent:
    event: AnalyticsEvent
    previous: Session | None
    following: Session


class SessionEngine:
    """Propose transitions without committing session state before delivery."""

    def __init__(self, settings: EventSettings) -> None:
        self.settings = settings
        self.random = Random(settings.random_seed)
        self.users = UserPool(settings, self.random)
        self.sessions: deque[Session] = deque()
        self.pending: PreparedEvent | None = None

    def prepare(self, snapshot: SourceSnapshot, now: datetime) -> PreparedEvent | None:
        if now.utcoffset() is None:
            raise ValueError("Event time must be timezone-aware")
        if self.pending is not None:
            return self.pending

        busy = {session.context.installation_id for session in self.sessions}
        eligible = [
            context
            for context in snapshot.contexts
            if context.installation_id not in busy
        ]
        previous = None
        if (
            len(self.sessions) < self.settings.max_active_sessions
            and eligible
            and snapshot.track_ids
        ):
            session = Session(
                context=self.random.choice(eligible),
                session_id=uuid4(),
                user_id=self.users.choose(),
                target_tracks=self.random.randint(
                    self.settings.session_min_tracks, self.settings.session_max_tracks
                ),
            )
        elif self.sessions:
            previous = session = self.sessions[0]
        else:
            return None

        if session.ended:
            raise RuntimeError("A completed session must not produce more events")
        if not session.started:
            kind = EventType.session_started
            following = replace(session, started=True)
        elif session.reaction is not None:
            kind = session.reaction
            following = replace(session, reaction=None)
        elif session.selected_tracks < session.target_tracks:
            if not snapshot.track_ids:
                return None
            kind = EventType.track_selected
            reaction = None
            if session.user_id is not None:
                chance = self.random.randrange(100)
                if chance < self.settings.like_percent:
                    reaction = EventType.track_liked
                elif (
                    chance < self.settings.like_percent + self.settings.dislike_percent
                ):
                    reaction = EventType.track_disliked
            following = replace(
                session,
                selected_tracks=session.selected_tracks + 1,
                track_id=self.random.choice(snapshot.track_ids),
                playback_id=uuid4(),
                reaction=reaction,
            )
        else:
            kind = EventType.session_ended
            following = replace(session, ended=True, track_id=None, playback_id=None)

        now = now.astimezone(UTC)
        event = AnalyticsEvent(
            event_id=uuid7(),
            event_type=kind,
            device_id=session.context.device_id,
            device_os_id=session.context.device_os_id,
            app_release_id=session.context.app_release_id,
            session_id=session.session_id,
            event_timestamp=now,
            received_at=now,
            user_id=session.user_id,
            track_id=following.track_id,
            playback_id=following.playback_id,
        )
        self.pending = PreparedEvent(event, previous, following)
        return self.pending

    def confirm(self, prepared: PreparedEvent) -> None:
        if self.pending is not prepared:
            raise ValueError("Only the pending delivery can advance session state")
        if prepared.previous is not None:
            if not self.sessions or self.sessions[0] is not prepared.previous:
                raise RuntimeError("Session delivery order changed")
            self.sessions.popleft()
        if not prepared.following.ended:
            self.sessions.append(prepared.following)
        self.pending = None
