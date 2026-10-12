from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
from src.loaders import (
    DimAppReleaseLoader,
    DimArtistLoader,
    DimDeviceLoader,
    DimDeviceOsLoader,
    DimEventTypeLoader,
    DimGenreLoader,
    DimReleaseLoader,
    DimTrackLoader,
)
from src.models.dwh import (
    BrgArtistRelease,
    BrgDeviceAppRelease,
    BrgTrackArtist,
    BrgTrackGenre,
    BrgTrackRelease,
    ClickHouseModel,
    DimAppRelease,
    DimArtist,
    DimDevice,
    DimDeviceOs,
    DimEventType,
    DimGenre,
    DimRelease,
    DimTrack,
    FctEvent,
)
from src.models.kafka.analytics_event import AnalyticsEvent, EventType
from src.models.oltp import (
    AppRelease,
    Artist,
    ArtistRelease,
    Base,
    Device,
    DeviceOsHistory,
    Genre,
    Installation,
    Release,
    Track,
    TrackArtist,
    TrackGenre,
    TrackRelease,
)

TIMESTAMP = datetime(2026, 10, 11, 12, 0, 0, 123000, tzinfo=UTC)
LOAD_ID = UUID(int=9000)
STORED_LOAD_ID = UUID(int=9001)
ENTITY_IDS = {
    "device": (UUID(int=100), UUID(int=101)),
    "device_os": (UUID(int=200), UUID(int=201)),
    "app_release": (UUID(int=300), UUID(int=301)),
    "track": (UUID(int=400), UUID(int=401)),
    "artist": (UUID(int=500), UUID(int=501)),
    "genre": (UUID(int=600), UUID(int=601)),
    "release": (UUID(int=700), UUID(int=701)),
}
ENTITY_SKS = {
    "device": (0, 11),
    "device_os": (0, 21),
    "app_release": (0, 31),
    "track": (0, 41),
    "artist": (0, 51),
    "genre": (0, 61),
    "release": (0, 71),
}
DIMENSIONS = {
    "device": (DimDeviceLoader, "updated_at"),
    "device_os": (DimDeviceOsLoader, "valid_from"),
    "app_release": (DimAppReleaseLoader, "source_created_at"),
    "track": (DimTrackLoader, "updated_at"),
    "artist": (DimArtistLoader, "updated_at"),
    "genre": (DimGenreLoader, "updated_at"),
    "release": (DimReleaseLoader, "updated_at"),
}
EVENT_TYPE_SKS = {"track_selected": 0, "session_started": 81}


def make_dimension_batch(
    entity_name: str, *, missing_sk: bool = False
) -> tuple[list[Base], list[ClickHouseModel]]:
    sources = []
    expected = []
    for index in range(2):
        entity_id = ENTITY_IDS[entity_name][index]
        sk = None if missing_sk and index == 1 else ENTITY_SKS[entity_name][index]
        created_at = TIMESTAMP + timedelta(seconds=index)
        updated_at = created_at + timedelta(minutes=1)
        deleted_at = updated_at + timedelta(days=1) if index else None
        if entity_name == "device":
            source = Device(
                id=entity_id,
                name=f"Phone {index}",
                device_version=f"v{index}",
                created_at=created_at,
                updated_at=updated_at,
            )
            target = DimDevice(
                device_sk=sk,
                device_id=entity_id,
                name=f"Phone {index}",
                device_version=f"v{index}",
                load_id=LOAD_ID,
                source_created_at=created_at,
                updated_at=updated_at,
            )
        elif entity_name == "device_os":
            valid_from = created_at - timedelta(days=1)
            source = DeviceOsHistory(
                id=entity_id,
                device_id=ENTITY_IDS["device"][index],
                os_name="iOS" if index else "Android",
                os_version=f"{16 + index}.1",
                valid_from=valid_from,
                created_at=created_at,
            )
            target = DimDeviceOs(
                device_os_sk=sk,
                device_os_id=entity_id,
                name="iOS" if index else "Android",
                device_os_version=f"{16 + index}.1",
                valid_from=valid_from,
                load_id=LOAD_ID,
                source_created_at=created_at,
            )
        elif entity_name == "app_release":
            released_at = created_at - timedelta(days=2)
            source = AppRelease(
                id=entity_id,
                platform="ios" if index else "android",
                app_release_version=f"2.{index}.0",
                build=f"build-{index}",
                released_at=released_at,
                created_at=created_at,
            )
            target = DimAppRelease(
                app_release_sk=sk,
                app_release_id=entity_id,
                platform="ios" if index else "android",
                app_release_version=f"2.{index}.0",
                build=f"build-{index}",
                released_at=released_at,
                load_id=LOAD_ID,
                source_created_at=created_at,
            )
        elif entity_name == "track":
            source = Track(
                id=entity_id,
                name=f"Track {index} 🎵",
                duration_ms=180000 + index,
                image_url=f"https://example.test/track-{index}.png",
                is_available=bool(index),
                created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
            target = DimTrack(
                track_sk=sk,
                track_id=entity_id,
                name=f"Track {index} 🎵",
                duration_ms=180000 + index,
                is_available=bool(index),
                load_id=LOAD_ID,
                source_created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
        elif entity_name == "artist":
            source = Artist(
                id=entity_id,
                nickname=f"Artist {index} 🎸",
                name="First name" if index else None,
                middle_name="Middle name" if index else None,
                last_name="Last name" if index else None,
                country_code="GB" if index else None,
                biography="Artist biography" if index else None,
                image_url=None,
                is_verified=bool(index),
                created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
            target = DimArtist(
                artist_sk=sk,
                artist_id=entity_id,
                nickname=f"Artist {index} 🎸",
                name="First name" if index else None,
                middle_name="Middle name" if index else None,
                last_name="Last name" if index else None,
                country_code="GB" if index else None,
                biography="Artist biography" if index else None,
                is_verified=bool(index),
                load_id=LOAD_ID,
                source_created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
        elif entity_name == "genre":
            source = Genre(
                id=entity_id,
                name=f"Genre {index}",
                created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
            target = DimGenre(
                genre_sk=sk,
                genre_id=entity_id,
                name=f"Genre {index}",
                load_id=LOAD_ID,
                source_created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
        elif entity_name == "release":
            released_at = created_at - timedelta(days=3)
            source = Release(
                id=entity_id,
                name=f"Album {index} 🎶",
                is_available=bool(index),
                released_at=released_at,
                created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
            target = DimRelease(
                release_sk=sk,
                release_id=entity_id,
                name=f"Album {index} 🎶",
                is_available=bool(index),
                released_at=released_at,
                load_id=LOAD_ID,
                source_created_at=created_at,
                updated_at=updated_at,
                deleted_at=deleted_at,
            )
        else:
            raise ValueError(f"Unknown dimension: {entity_name}")
        sources.append(source)
        expected.append(target)
    return (
        [sources[1], sources[0], sources[1]],
        [expected[1], expected[0], expected[1]],
    )


def make_dimension_rows(entity_name: str) -> list[ClickHouseModel]:
    _, expected = make_dimension_batch(entity_name)
    _, version_column = DIMENSIONS[entity_name]
    rows = []
    for target in expected[:2]:
        values = asdict(target)
        sk = values[f"{entity_name}_sk"]
        latest = values[version_column]
        earlier = latest - timedelta(hours=1)
        versions = [(latest, sk), (earlier, sk + 1000)]
        if sk:
            versions.extend([(latest, sk - 1), (earlier, sk), (earlier, sk - 1)])
        values["load_id"] = STORED_LOAD_ID
        if "name" in values:
            values["name"] = "Stored dimension name"
        if "nickname" in values:
            values["nickname"] = "Stored artist nickname"
        for version, candidate_sk in versions:
            rows.append(
                type(target)(
                    **{
                        **values,
                        f"{entity_name}_sk": candidate_sk,
                        version_column: version,
                    }
                )
            )
    return rows


async def clear_dimensions(client: AsyncClient, entity_names: tuple[str, ...]) -> None:
    for entity_name in entity_names:
        loader_class = (
            DimEventTypeLoader
            if entity_name == "event_type"
            else DIMENSIONS[entity_name][0]
        )
        await client.command(f"TRUNCATE TABLE {loader_class.model.table_to_insert()}")


async def seed_dimensions(
    client: AsyncClient,
    entity_names: tuple[str, ...],
    *,
    reverse_rows: bool = False,
    missing_entity_name: str | None = None,
) -> None:
    await clear_dimensions(client, entity_names)
    for entity_name in entity_names:
        if entity_name == "event_type":
            loader_class = DimEventTypeLoader
            rows = [
                DimEventType(event_type_sk=sk, name=name, load_id=STORED_LOAD_ID)
                for name, sk in EVENT_TYPE_SKS.items()
                if not (
                    missing_entity_name == "event_type" and name == "track_selected"
                )
            ]
        else:
            loader_class, _ = DIMENSIONS[entity_name]
            rows = make_dimension_rows(entity_name)
            if entity_name == missing_entity_name:
                rows = [
                    row
                    for row in rows
                    if getattr(row, f"{entity_name}_id") != ENTITY_IDS[entity_name][1]
                ]
        if reverse_rows:
            rows.reverse()
        written = await loader_class(client).load_batch(rows)
        assert written == len(rows)


def missing_dimension_message(entity_name: str) -> str:
    if entity_name == "event_type":
        return "Dimension not found in music_dwh.dim_event_type: name=track_selected"
    loader_class, _ = DIMENSIONS[entity_name]
    return (
        f"Dimension not found in {loader_class.model.table_to_insert()}: "
        f"{entity_name}_id={ENTITY_IDS[entity_name][1]}"
    )


def make_bridge_batch(
    bridge_name: str,
) -> tuple[list[Base], list[ClickHouseModel]]:
    sources = []
    expected = []
    for index in range(2):
        other = 1 - index
        created_at = TIMESTAMP + timedelta(seconds=index)
        updated_at = created_at + timedelta(minutes=1)
        if bridge_name == "device_app_release":
            source = Installation(
                id=UUID(int=800 + index),
                device_id=ENTITY_IDS["device"][index],
                app_release_id=ENTITY_IDS["app_release"][other],
                installed_at=created_at - timedelta(days=1),
                last_seen_at=updated_at if index else None,
                created_at=created_at,
                updated_at=updated_at,
            )
            target = BrgDeviceAppRelease(
                device_sk=ENTITY_SKS["device"][index],
                app_release_sk=ENTITY_SKS["app_release"][other],
                load_id=LOAD_ID,
            )
        elif bridge_name == "track_genre":
            source = TrackGenre(
                track_id=ENTITY_IDS["track"][index],
                genre_id=ENTITY_IDS["genre"][other],
                created_at=created_at,
            )
            target = BrgTrackGenre(
                track_sk=ENTITY_SKS["track"][index],
                genre_sk=ENTITY_SKS["genre"][other],
                load_id=LOAD_ID,
            )
        elif bridge_name == "track_artist":
            source = TrackArtist(
                track_id=ENTITY_IDS["track"][index],
                artist_id=ENTITY_IDS["artist"][other],
                artist_number=index + 1,
                created_at=created_at,
                updated_at=updated_at,
            )
            target = BrgTrackArtist(
                track_sk=ENTITY_SKS["track"][index],
                artist_sk=ENTITY_SKS["artist"][other],
                artist_number=index + 1,
                load_id=LOAD_ID,
            )
        elif bridge_name == "track_release":
            source = TrackRelease(
                track_id=ENTITY_IDS["track"][index],
                release_id=ENTITY_IDS["release"][other],
                track_number=index + 1,
                disc_number=index + 1 if index else None,
                created_at=created_at,
                updated_at=updated_at,
            )
            target = BrgTrackRelease(
                track_sk=ENTITY_SKS["track"][index],
                release_sk=ENTITY_SKS["release"][other],
                track_number=index + 1,
                disc_number=index + 1 if index else None,
                load_id=LOAD_ID,
            )
        elif bridge_name == "artist_release":
            source = ArtistRelease(
                artist_id=ENTITY_IDS["artist"][index],
                release_id=ENTITY_IDS["release"][other],
                artist_number=index + 1,
                created_at=created_at,
                updated_at=updated_at,
            )
            target = BrgArtistRelease(
                artist_sk=ENTITY_SKS["artist"][index],
                release_sk=ENTITY_SKS["release"][other],
                artist_number=index + 1,
                load_id=LOAD_ID,
            )
        else:
            raise ValueError(f"Unknown bridge: {bridge_name}")
        sources.append(source)
        expected.append(target)
    return (
        [sources[1], sources[0], sources[1]],
        [expected[1], expected[0], expected[1]],
    )


def make_event_batch() -> tuple[list[AnalyticsEvent], list[FctEvent]]:
    sources = []
    expected = []
    for index in range(2):
        other = 1 - index
        timestamp = TIMESTAMP + timedelta(seconds=index)
        sources.append(
            AnalyticsEvent(
                event_id=UUID(int=1000 + index),
                event_type=EventType.track_selected,
                device_id=ENTITY_IDS["device"][index],
                device_os_id=ENTITY_IDS["device_os"][other],
                app_release_id=ENTITY_IDS["app_release"][index],
                track_id=ENTITY_IDS["track"][other],
                user_id=UUID(int=1100 + index) if index else None,
                session_id=UUID(int=1200 + index),
                playback_id=UUID(int=1300 + index) if index else None,
                event_timestamp=timestamp,
                received_at=timestamp + timedelta(milliseconds=456),
            )
        )
        expected.append(
            FctEvent(
                event_sk=None,
                event_id=UUID(int=1000 + index),
                event_type_sk=0,
                device_sk=ENTITY_SKS["device"][index],
                device_os_sk=ENTITY_SKS["device_os"][other],
                app_release_sk=ENTITY_SKS["app_release"][index],
                track_sk=ENTITY_SKS["track"][other],
                user_id=UUID(int=1100 + index) if index else None,
                session_id=UUID(int=1200 + index),
                playback_id=UUID(int=1300 + index) if index else None,
                event_timestamp=timestamp,
                received_at=timestamp + timedelta(milliseconds=456),
                load_id=LOAD_ID,
            )
        )
    sources.append(
        AnalyticsEvent(
            event_id=UUID(int=1002),
            event_type=EventType.session_started,
            device_id=ENTITY_IDS["device"][0],
            device_os_id=ENTITY_IDS["device_os"][0],
            app_release_id=ENTITY_IDS["app_release"][0],
            session_id=UUID(int=1202),
            event_timestamp=TIMESTAMP + timedelta(minutes=1),
            received_at=TIMESTAMP + timedelta(minutes=1, milliseconds=456),
        )
    )
    expected.append(
        FctEvent(
            event_sk=None,
            event_id=UUID(int=1002),
            event_type_sk=81,
            device_sk=0,
            device_os_sk=0,
            app_release_sk=0,
            track_sk=None,
            user_id=None,
            session_id=UUID(int=1202),
            playback_id=None,
            event_timestamp=TIMESTAMP + timedelta(minutes=1),
            received_at=TIMESTAMP + timedelta(minutes=1, milliseconds=456),
            load_id=LOAD_ID,
        )
    )
    return (
        [sources[1], sources[0], sources[1], sources[2]],
        [expected[1], expected[0], expected[1], expected[2]],
    )
