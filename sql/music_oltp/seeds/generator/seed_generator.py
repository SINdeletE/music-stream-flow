from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Any
from uuid import UUID, uuid4

from faker import Faker


DEVICE_FAMILIES = (
    ("Apple iPhone", ("13", "14", "15", "15 Pro", "16", "16 Pro"), "iOS", ("16", "17", "18", "26"), "ios"),
    ("Samsung Galaxy", ("S22", "S23", "S24", "S25", "A54", "A55"), "Android", ("12", "13", "14", "15"), "android"),
    ("Google Pixel", ("7", "7 Pro", "8", "8 Pro", "9", "9 Pro"), "Android", ("13", "14", "15"), "android"),
    ("Xiaomi", ("12", "13", "14", "14 Ultra", "14T Pro"), "Android", ("12", "13", "14", "15"), "android"),
    ("OnePlus", ("10 Pro", "11", "12", "13", "Nord 4"), "Android", ("12", "13", "14", "15"), "android"),
)

APP_PLATFORMS = ("ios", "android")

GENRE_NAMES = (
    "Alternative",
    "Ambient",
    "Blues",
    "Classical",
    "Country",
    "Dance",
    "Disco",
    "Drum and Bass",
    "Electronic",
    "Folk",
    "Funk",
    "Hip-Hop",
    "House",
    "Indie",
    "Jazz",
    "Latin",
    "Metal",
    "Pop",
    "R&B",
    "Reggae",
    "Rock",
    "Soul",
    "Techno",
    "Trance",
)

DEFAULT_DEVICE_COUNT = 40
DEFAULT_APP_RELEASE_COUNT = 12
DEFAULT_INSTALLATION_COUNT = 200
DEFAULT_GENRE_COUNT = 20
DEFAULT_ARTIST_COUNT = 120
DEFAULT_RELEASE_COUNT = 80
DEFAULT_TRACK_COUNT = 500
DEFAULT_RANDOM_SEED = 42

MIN_TRACK_DURATION_MS = 60_000
MAX_TRACK_DURATION_MS = 8 * 60_000
TRACKS_PER_DISC = 12
MAX_ARTISTS_PER_TRACK = 3
MAX_ARTISTS_PER_RELEASE = 4
MAX_GENRES_PER_TRACK = 3
MAX_RELEASES_PER_TRACK = 2

Row = dict[str, Any]


@dataclass(frozen=True, slots=True)
class SeedCounts:
    devices: int = DEFAULT_DEVICE_COUNT
    app_releases: int = DEFAULT_APP_RELEASE_COUNT
    installations: int = DEFAULT_INSTALLATION_COUNT
    genres: int = DEFAULT_GENRE_COUNT
    artists: int = DEFAULT_ARTIST_COUNT
    releases: int = DEFAULT_RELEASE_COUNT
    tracks: int = DEFAULT_TRACK_COUNT

    def validate(self) -> None:
        for count_field in fields(self):
            name = count_field.name
            value = getattr(self, name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")

        if self.installations and (not self.devices or not self.app_releases):
            raise ValueError(
                "installations require at least one device and one app release",
            )

        if self.tracks and (not self.artists or not self.releases):
            raise ValueError(
                "tracks require at least one artist and one release",
            )


@dataclass(slots=True)
class SeedData:
    device: list[Row]
    app_release: list[Row]
    installation: list[Row]
    track: list[Row]
    genre: list[Row]
    artist: list[Row]
    release: list[Row]
    track_genre: list[Row]
    track_artist: list[Row]
    track_release: list[Row]
    artist_release: list[Row]

    def tables(self) -> tuple[tuple[str, list[Row]], ...]:
        return (
            ("device", self.device),
            ("app_release", self.app_release),
            ("installation", self.installation),
            ("track", self.track),
            ("genre", self.genre),
            ("artist", self.artist),
            ("release", self.release),
            ("track_genre", self.track_genre),
            ("track_artist", self.track_artist),
            ("track_release", self.track_release),
            ("artist_release", self.artist_release),
        )


class MusicOltpSeedGenerator:
    def __init__(self, seed: int = DEFAULT_RANDOM_SEED) -> None:
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)
        self.random = Random(seed)
        self.now = datetime.now(timezone.utc)

    def generate(self, counts: SeedCounts) -> SeedData:
        counts.validate()

        devices = self._generate_devices(counts.devices)
        app_releases = self._generate_app_releases(counts.app_releases)
        installations = self._generate_installations(
            counts.installations,
            devices,
            app_releases,
        )
        genres = self._generate_genres(counts.genres)
        artists = self._generate_artists(counts.artists)
        releases = self._generate_releases(counts.releases)
        tracks = self._generate_tracks(counts.tracks)
        track_genres = self._generate_track_genres(tracks, genres)
        track_artists, artists_by_track = self._generate_track_artists(
            tracks,
            artists,
        )
        track_releases, artists_by_release = self._generate_track_releases(
            tracks,
            releases,
            artists_by_track,
        )
        artist_releases = self._generate_artist_releases(
            releases,
            artists,
            artists_by_release,
        )

        return SeedData(
            device=devices,
            app_release=app_releases,
            installation=installations,
            track=tracks,
            genre=genres,
            artist=artists,
            release=releases,
            track_genre=track_genres,
            track_artist=track_artists,
            track_release=track_releases,
            artist_release=artist_releases,
        )

    def _generate_devices(self, count: int) -> list[Row]:
        rows: list[Row] = []
        for index in range(count):
            name, versions, os_name, os_versions, platform = DEVICE_FAMILIES[
                index % len(DEVICE_FAMILIES)
            ]
            created_at, updated_at, _ = self._lifecycle(max_age_days=1_000)
            rows.append(
                {
                    "id": self._uuid(),
                    "name": name,
                    "device_version": self.random.choice(versions),
                    "os_name": os_name,
                    "os_version": self.random.choice(os_versions),
                    "platform": platform,
                    "created_at": created_at,
                    "updated_at": updated_at,
                },
            )
        return rows

    def _generate_app_releases(self, count: int) -> list[Row]:
        rows: list[Row] = []
        for index in range(count):
            platform = APP_PLATFORMS[index % len(APP_PLATFORMS)]
            released_at = self._between(
                self.now - timedelta(days=1_000),
                self.now - timedelta(days=1),
            )
            created_at = self._between(
                released_at - timedelta(days=30),
                released_at,
            )
            rows.append(
                {
                    "id": self._uuid(),
                    "platform": platform,
                    "app_release_version": (
                        f"{1 + index // 10}.{index % 10}."
                        f"{self.random.randint(0, 9)}"
                    ),
                    "build": str(1_000 + index),
                    "released_at": released_at,
                    "created_at": created_at,
                },
            )
        return rows

    def _generate_installations(
        self,
        count: int,
        devices: list[Row],
        app_releases: list[Row],
    ) -> list[Row]:
        if not count:
            return []

        devices_by_platform = self._group_by(devices, "platform")
        releases_by_platform = self._group_by(app_releases, "platform")
        compatible_platforms = tuple(
            sorted(devices_by_platform.keys() & releases_by_platform.keys()),
        )
        if not compatible_platforms:
            raise ValueError("devices and app releases have no compatible platforms")

        rows: list[Row] = []
        for _ in range(count):
            platform = self.random.choice(compatible_platforms)
            device = self.random.choice(devices_by_platform[platform])
            app_release = self.random.choice(releases_by_platform[platform])
            installed_at = self._between(
                max(device["created_at"], app_release["released_at"]),
                self.now,
            )
            rows.append(
                {
                    "id": self._uuid(),
                    "device_id": device["id"],
                    "app_release_id": app_release["id"],
                    "installed_at": installed_at,
                    "created_at": installed_at,
                    "updated_at": self._between(installed_at, self.now),
                },
            )
        return rows

    def _generate_genres(self, count: int) -> list[Row]:
        rows: list[Row] = []
        for index in range(count):
            created_at, updated_at, deleted_at = self._lifecycle(
                max_age_days=3_000,
                deletion_chance=2,
            )
            name = (
                GENRE_NAMES[index]
                if index < len(GENRE_NAMES)
                else f"Genre {index + 1}"
            )
            rows.append(
                {
                    "id": self._uuid(),
                    "name": name,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": deleted_at,
                },
            )
        return rows

    def _generate_artists(self, count: int) -> list[Row]:
        rows: list[Row] = []
        for index in range(count):
            created_at, updated_at, deleted_at = self._lifecycle(
                max_age_days=3_000,
                deletion_chance=3,
            )
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            artist_id = self._uuid()
            rows.append(
                {
                    "id": artist_id,
                    "nickname": f"{self.fake.user_name()}_{index + 1}",
                    "name": first_name if self._chance(75) else None,
                    "middle_name": self.fake.first_name() if self._chance(20) else None,
                    "last_name": last_name if self._chance(75) else None,
                    "country_code": self.fake.country_code() if self._chance(90) else None,
                    "biography": self.fake.text(max_nb_chars=400) if self._chance(70) else None,
                    "image_url": self._image_url("artists", artist_id) if self._chance(80) else None,
                    "is_verified": self._chance(15),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": deleted_at,
                },
            )
        return rows

    def _generate_releases(self, count: int) -> list[Row]:
        rows: list[Row] = []
        for _ in range(count):
            created_at, updated_at, deleted_at = self._lifecycle(
                max_age_days=2_500,
                deletion_chance=2,
            )
            released_at = self._between(created_at, self.now)
            rows.append(
                {
                    "id": self._uuid(),
                    "name": self.fake.sentence(nb_words=3).rstrip("."),
                    "is_available": deleted_at is None and self._chance(90),
                    "released_at": released_at,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": deleted_at,
                },
            )
        return rows

    def _generate_tracks(self, count: int) -> list[Row]:
        rows: list[Row] = []
        for _ in range(count):
            created_at, updated_at, deleted_at = self._lifecycle(
                max_age_days=2_500,
                deletion_chance=2,
            )
            track_id = self._uuid()
            rows.append(
                {
                    "id": track_id,
                    "name": self.fake.sentence(nb_words=4).rstrip("."),
                    "duration_ms": self.random.randint(
                        MIN_TRACK_DURATION_MS,
                        MAX_TRACK_DURATION_MS,
                    ),
                    "image_url": self._image_url("tracks", track_id) if self._chance(80) else None,
                    "is_available": deleted_at is None and self._chance(90),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": deleted_at,
                },
            )
        return rows

    def _generate_track_genres(
        self,
        tracks: list[Row],
        genres: list[Row],
    ) -> list[Row]:
        if not genres:
            return []

        rows: list[Row] = []
        for track in tracks:
            genre_count = (
                0
                if self._chance(15)
                else self.random.randint(1, min(MAX_GENRES_PER_TRACK, len(genres)))
            )
            for genre in self.random.sample(genres, genre_count):
                rows.append(
                    {
                        "track_id": track["id"],
                        "genre_id": genre["id"],
                        "created_at": self._between(
                            max(track["created_at"], genre["created_at"]),
                            self.now,
                        ),
                    },
                )
        return rows

    def _generate_track_artists(
        self,
        tracks: list[Row],
        artists: list[Row],
    ) -> tuple[list[Row], dict[UUID, list[UUID]]]:
        rows: list[Row] = []
        artists_by_track: dict[UUID, list[UUID]] = {}
        for track in tracks:
            artist_count = self.random.randint(
                1,
                min(MAX_ARTISTS_PER_TRACK, len(artists)),
            )
            selected_artists = self.random.sample(artists, artist_count)
            artists_by_track[track["id"]] = [artist["id"] for artist in selected_artists]
            for artist_number, artist in enumerate(selected_artists, start=1):
                created_at = self._between(
                    max(track["created_at"], artist["created_at"]),
                    self.now,
                )
                rows.append(
                    {
                        "track_id": track["id"],
                        "artist_id": artist["id"],
                        "artist_number": artist_number,
                        "created_at": created_at,
                        "updated_at": self._between(created_at, self.now),
                    },
                )
        return rows, artists_by_track

    def _generate_track_releases(
        self,
        tracks: list[Row],
        releases: list[Row],
        artists_by_track: dict[UUID, list[UUID]],
    ) -> tuple[list[Row], dict[UUID, list[UUID]]]:
        rows: list[Row] = []
        artists_by_release: dict[UUID, list[UUID]] = {
            release["id"]: [] for release in releases
        }
        positions_by_release: dict[UUID, int] = {
            release["id"]: 0 for release in releases
        }

        for track in tracks:
            release_count = self.random.randint(
                1,
                min(MAX_RELEASES_PER_TRACK, len(releases)),
            )
            for release in self.random.sample(releases, release_count):
                release_id = release["id"]
                positions_by_release[release_id] += 1
                position = positions_by_release[release_id]
                created_at = self._between(
                    max(track["created_at"], release["created_at"]),
                    self.now,
                )
                rows.append(
                    {
                        "track_id": track["id"],
                        "release_id": release_id,
                        "track_number": (position - 1) % TRACKS_PER_DISC + 1,
                        "disc_number": (position - 1) // TRACKS_PER_DISC + 1,
                        "created_at": created_at,
                        "updated_at": self._between(created_at, self.now),
                    },
                )

                known_artist_ids = artists_by_release[release_id]
                for artist_id in artists_by_track[track["id"]]:
                    if artist_id not in known_artist_ids:
                        known_artist_ids.append(artist_id)

        return rows, artists_by_release

    def _generate_artist_releases(
        self,
        releases: list[Row],
        artists: list[Row],
        artists_by_release: dict[UUID, list[UUID]],
    ) -> list[Row]:
        if not artists:
            return []

        artist_by_id = {artist["id"]: artist for artist in artists}
        rows: list[Row] = []
        for release in releases:
            credited_artist_ids = artists_by_release[release["id"]]
            if not credited_artist_ids:
                count = self.random.randint(
                    1,
                    min(MAX_ARTISTS_PER_RELEASE, len(artists)),
                )
                credited_artist_ids = [
                    artist["id"] for artist in self.random.sample(artists, count)
                ]

            for artist_number, artist_id in enumerate(credited_artist_ids, start=1):
                artist = artist_by_id[artist_id]
                created_at = self._between(
                    max(artist["created_at"], release["created_at"]),
                    self.now,
                )
                rows.append(
                    {
                        "artist_id": artist_id,
                        "release_id": release["id"],
                        "artist_number": artist_number,
                        "created_at": created_at,
                        "updated_at": self._between(created_at, self.now),
                    },
                )
        return rows

    def _lifecycle(
        self,
        *,
        max_age_days: int,
        deletion_chance: int = 0,
    ) -> tuple[datetime, datetime, datetime | None]:
        created_at = self._between(
            self.now - timedelta(days=max_age_days),
            self.now,
        )
        updated_at = self._between(created_at, self.now)
        deleted_at = (
            self._between(updated_at, self.now)
            if deletion_chance and self._chance(deletion_chance)
            else None
        )
        return created_at, updated_at, deleted_at

    def _between(self, start: datetime, end: datetime) -> datetime:
        if start >= end:
            return end
        seconds = self.random.uniform(0, (end - start).total_seconds())
        return start + timedelta(seconds=seconds)

    def _chance(self, percentage: int) -> bool:
        return self.random.random() < percentage / 100

    def _uuid(self) -> UUID:
        return uuid4()

    @staticmethod
    def _image_url(entity: str, entity_id: UUID) -> str:
        return f"https://cdn.example.test/{entity}/{entity_id}.jpg"

    @staticmethod
    def _group_by(rows: list[Row], key: str) -> dict[str, list[Row]]:
        grouped: dict[str, list[Row]] = {}
        for row in rows:
            grouped.setdefault(row[key], []).append(row)
        return grouped
