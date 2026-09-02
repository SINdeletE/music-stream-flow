from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid7

from faker import Faker

from .models import AnalyticsEvent, DeviceInfo, EventType, PlaybackInfo
from .models.devices import devices
from .models.playback_info import AudioQuality
from .models.seeked_info import SeekedInfo


fake = Faker()

EVENT_TYPES = tuple(EventType)

SESSION_EVENT_TYPES = {
    EventType.session_started,
    EventType.session_ended,
}

TRACK_EVENT_TYPES = set(EventType) - SESSION_EVENT_TYPES

PLAYBACK_EVENT_TYPES = {
    EventType.track_started,
    EventType.track_paused,
    EventType.track_skipped,
    EventType.track_completed,
}

PLAYBACK_IDENTIFIED_EVENT_TYPES = PLAYBACK_EVENT_TYPES | {
    EventType.track_seeked,
}

IOS_VERSIONS = ("16", "17", "18", "26")
HARMONY_OS_VERSIONS = ("3", "4", "5")
ANDROID_VERSIONS = ("12", "13", "14", "15")
DEVICE_VERSIONS = ("1.0", "1.1", "2.0")

REPEAT_MODES = ("off", "track", "context")
PLAYBACK_SPEEDS = (0.75, 1.0, 1.25, 1.5)
AUDIO_QUALITIES = tuple(AudioQuality)

MIN_TRACK_POSITION_MS = 0
MAX_TRACK_POSITION_MS = 300_000
MIN_VOLUME = 0
MAX_VOLUME = 100
SHUFFLE_ENABLED_CHANCE = 35
OFFLINE_PLAYBACK_CHANCE = 10
USER_INITIATED_SEEK_CHANCE = 95
TRACK_STARTED_POSITION_MS = 0
MIN_RECEIVE_DELAY_MS = 5
MAX_RECEIVE_DELAY_MS = 2_500
EVENT_HISTORY_PERIOD = "-30d"
EVENT_HISTORY_END = "now"
PROPERTIES_SCHEMA_VERSION = 1


def generate_device_info() -> DeviceInfo:
    """Generate device information using the DeviceInfo model."""
    device_name = str(fake.random_element(elements=devices))
    device_os, device_os_version = _generate_device_os(device_name)

    return DeviceInfo(
        device_name=device_name,
        device_version=str(fake.random_element(elements=DEVICE_VERSIONS)),
        device_os=device_os,
        device_os_version=device_os_version,
    )


def generate_playback_info(
    *,
    playback_position_ms: int | None = None,
) -> PlaybackInfo:
    """Generate playback information using the PlaybackInfo model."""
    return PlaybackInfo(
        playback_position_ms=(
            playback_position_ms
            if playback_position_ms is not None
            else fake.random_int(
                min=MIN_TRACK_POSITION_MS,
                max=MAX_TRACK_POSITION_MS,
            )
        ),
        playback_volume=fake.random_int(min=MIN_VOLUME, max=MAX_VOLUME),
        playback_shuffle_enabled=fake.boolean(
            chance_of_getting_true=SHUFFLE_ENABLED_CHANCE,
        ),
        playback_repeat_mode=str(fake.random_element(elements=REPEAT_MODES)),
        playback_speed=float(fake.random_element(elements=PLAYBACK_SPEEDS)),
        playback_is_offline=fake.boolean(
            chance_of_getting_true=OFFLINE_PLAYBACK_CHANCE,
        ),
        audio_quality=fake.random_element(elements=AUDIO_QUALITIES),
    )


def generate_seeked_info(
    *,
    seeked_from_ms: int | None = None,
    seeked_to_ms: int | None = None,
    is_user_initiated: bool | None = None,
) -> SeekedInfo:
    """Generate seek information using the SeekedInfo model."""
    generated_from_ms = (
        seeked_from_ms
        if seeked_from_ms is not None
        else fake.random_int(
            min=MIN_TRACK_POSITION_MS,
            max=MAX_TRACK_POSITION_MS,
        )
    )
    generated_to_ms = (
        seeked_to_ms
        if seeked_to_ms is not None
        else fake.random_int(
            min=MIN_TRACK_POSITION_MS,
            max=MAX_TRACK_POSITION_MS,
        )
    )

    if seeked_to_ms is None and generated_to_ms == generated_from_ms:
        generated_to_ms = (
            MIN_TRACK_POSITION_MS
            if generated_from_ms != MIN_TRACK_POSITION_MS
            else MAX_TRACK_POSITION_MS
        )

    return SeekedInfo(
        seeked_from_ms=generated_from_ms,
        seeked_to_ms=generated_to_ms,
        is_user_initiated=(
            is_user_initiated
            if is_user_initiated is not None
            else fake.boolean(
                chance_of_getting_true=USER_INITIATED_SEEK_CHANCE,
            )
        ),
    )


def generate_event_properties(
    event_type: EventType,
) -> dict[str, Any]:
    """Convert an event's Info model to properties."""
    if event_type == EventType.session_started:
        return asdict(generate_device_info()) # We can't change playback from other device

    if event_type == EventType.track_seeked:
        return asdict(generate_seeked_info())

    if event_type in PLAYBACK_EVENT_TYPES:
        started_position = (
            TRACK_STARTED_POSITION_MS
            if event_type == EventType.track_started
            else None
        )
        return asdict(
            generate_playback_info(playback_position_ms=started_position),
        )

    return {}


def generate_analytics_event(
    event_type: EventType | None = None,
    *,
    user_id: UUID | None = None,
    session_id: UUID | None = None,
    track_id: UUID | None = None,
    playback_id: UUID | None = None,
    properties_schema_version: int = PROPERTIES_SCHEMA_VERSION,
    event_timestamp: datetime | None = None,
    received_at: datetime | None = None,
) -> AnalyticsEvent:
    """Generate one analytics event.

    IDs and timestamps can be supplied to build a coherent event sequence. Missing
    values are generated automatically.
    """
    selected_type = event_type or fake.random_element(elements=EVENT_TYPES)
    generated_event_timestamp = event_timestamp or fake.date_time_between(
        start_date=EVENT_HISTORY_PERIOD,
        end_date=EVENT_HISTORY_END,
        tzinfo=timezone.utc,
    )
    event_received_at = received_at or generated_event_timestamp + timedelta(
        milliseconds=fake.random_int(
            min=MIN_RECEIVE_DELAY_MS,
            max=MAX_RECEIVE_DELAY_MS,
        ),
    )

    event_properties = generate_event_properties(selected_type)

    return AnalyticsEvent(
        event_id=uuid7(),
        event_type=selected_type,
        event_timestamp=generated_event_timestamp,
        received_at=event_received_at,
        user_id=user_id or uuid7(),
        session_id=session_id or uuid7(),
        track_id=(track_id or uuid7()) if selected_type in TRACK_EVENT_TYPES else None,
        playback_id=(
            playback_id or uuid7()
            if selected_type in PLAYBACK_IDENTIFIED_EVENT_TYPES
            else None
        ),
        properties_schema_version=properties_schema_version,
        properties=event_properties,
    )


def generate_analytics_events(
    count: int,
    event_type: EventType | None = None,
    *,
    user_id: UUID | None = None,
    session_id: UUID | None = None,
    track_id: UUID | None = None,
    playback_id: UUID | None = None,
    properties_schema_version: int = PROPERTIES_SCHEMA_VERSION,
) -> list[AnalyticsEvent]:
    """Generate a list of analytics events with optional shared identifiers."""
    if count < 0:
        raise ValueError("count must be non-negative")

    return [
        generate_analytics_event(
            event_type,
            user_id=user_id,
            session_id=session_id,
            track_id=track_id,
            playback_id=playback_id,
            properties_schema_version=properties_schema_version,
        )
        for _ in range(count)
    ]


def _generate_device_os(device_name: str) -> tuple[str, str]:
    if device_name.startswith("iPhone"):
        return "iOS", str(fake.random_element(elements=IOS_VERSIONS))

    if device_name.startswith("Huawei"):
        return "HarmonyOS", str(
            fake.random_element(elements=HARMONY_OS_VERSIONS),
        )

    return "Android", str(fake.random_element(elements=ANDROID_VERSIONS))
