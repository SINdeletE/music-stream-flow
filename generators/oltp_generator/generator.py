from collections.abc import Sequence
from datetime import datetime
from random import Random
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from faker import Faker

from .catalogs import Catalogs, Device
from .repository import Repository
from .settings import INSERT_DEFAULTS, UPDATE_TABLES


def catalog_id(kind: str, value: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"music-stream-flow/{kind}/{value}")


class OltpGenerator:
    def __init__(
        self, repository: Repository, catalogs: Catalogs, *, seed: int = 42
    ) -> None:
        self.repository = repository
        self.catalogs = catalogs
        self.random = Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)

    def count(self, table: str) -> int:
        return self.repository.count(table)

    def execute(
        self,
        table: str,
        operation: str,
        now: datetime,
        *,
        excluded: Sequence[UUID] = (),
    ) -> UUID | None:
        if now.utcoffset() is None:
            raise ValueError("Generation timestamps must be timezone-aware")
        allowed = INSERT_DEFAULTS if operation == "insert" else UPDATE_TABLES
        if operation not in ("insert", "update") or table not in allowed:
            raise ValueError("Unsupported generation operation")
        with self.repository.transaction():
            if operation == "update":
                return self._update(table, now, excluded)
            return getattr(self, f"_insert_{table}")(now)

    def _insert_device(self, now: datetime) -> UUID | None:
        existing = {
            (row["name"], row["device_version"])
            for row in self.repository.rows("device")
        }
        for device in self.catalogs.devices:
            if (device.name, device.device_version) in existing:
                continue
            entity_id = catalog_id("device", f"{device.name}/{device.device_version}")
            self.repository.insert(
                "device",
                {
                    "id": entity_id,
                    "name": device.name,
                    "device_version": device.device_version,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            self._insert_os_version(entity_id, device, device.os.versions[0], now)
            return entity_id
        return None

    def _insert_os_version(
        self, device_id: UUID, device: Device, version: str, now: datetime
    ) -> UUID:
        entity_id = catalog_id(
            "device_os_history", f"{device_id}/{device.os.name}/{version}"
        )
        self.repository.insert(
            "device_os_history",
            {
                "id": entity_id,
                "device_id": device_id,
                "os_name": device.os.name,
                "os_version": version,
                "valid_from": now,
                "created_at": now,
            },
        )
        return entity_id

    def _insert_device_os_history(self, now: datetime) -> UUID | None:
        devices = {
            (row["name"], row["device_version"]): row
            for row in self.repository.rows("device")
        }
        candidates = list(self.catalogs.devices)
        self.random.shuffle(candidates)
        for device in candidates:
            row = devices.get((device.name, device.device_version))
            if row is None or row["created_at"] > now:
                continue
            history = self.repository.rows("device_os_history", device_id=row["id"])
            if history and max(item["valid_from"] for item in history) >= now:
                continue
            if any(
                item["os_name"] != device.os.name
                or item["os_version"] not in device.os.versions
                for item in history
            ):
                continue
            last_index = max(
                (device.os.versions.index(item["os_version"]) for item in history),
                default=-1,
            )
            if last_index + 1 < len(device.os.versions):
                return self._insert_os_version(
                    row["id"], device, device.os.versions[last_index + 1], now
                )
        return None

    def _insert_genre(self, now: datetime) -> UUID | None:
        existing = {row["name"] for row in self.repository.rows("genre")}
        for name in self.catalogs.genres:
            if name not in existing:
                entity_id = catalog_id("genre", name)
                self.repository.insert(
                    "genre",
                    {
                        "id": entity_id,
                        "name": name,
                        "created_at": now,
                        "updated_at": now,
                        "deleted_at": None,
                    },
                )
                return entity_id
        return None

    def _insert_artist(self, now: datetime) -> UUID:
        entity_id = uuid4()
        self.repository.insert(
            "artist",
            {
                "id": entity_id,
                "nickname": self.fake.user_name(),
                "name": self.fake.first_name(),
                "middle_name": None,
                "last_name": self.fake.last_name(),
                "country_code": self.fake.country_code(),
                "biography": self.fake.text(max_nb_chars=450),
                "image_url": None,
                "is_verified": self.random.choice((True, False)),
                "created_at": now,
                "updated_at": now,
                "deleted_at": None,
            },
        )
        return entity_id

    def _insert_app_release(self, now: datetime) -> UUID:
        index = self.repository.count("app_release")
        platforms = tuple(
            dict.fromkeys(device.os.platform for device in self.catalogs.devices)
        )
        entity_id = uuid4()
        self.repository.insert(
            "app_release",
            {
                "id": entity_id,
                "platform": platforms[index % len(platforms)],
                "app_release_version": f"1.{index // 100}.{index % 100}",
                "build": now.strftime("%Y%m%d%H%M%S%f"),
                "released_at": now,
                "created_at": now,
            },
        )
        return entity_id

    def _insert_release(self, now: datetime) -> UUID | None:
        artists = [
            row
            for row in self.repository.sample("artist", self.random.randint(1, 3))
            if row["deleted_at"] is None and row["created_at"] <= now
        ]
        if not artists:
            return None
        entity_id = uuid4()
        self.repository.insert(
            "release",
            {
                "id": entity_id,
                "name": self.fake.sentence(nb_words=3).rstrip("."),
                "is_available": True,
                "released_at": now,
                "created_at": now,
                "updated_at": now,
                "deleted_at": None,
            },
        )
        for index, artist in enumerate(artists, 1):
            self.repository.insert(
                "artist_release",
                {
                    "artist_id": artist["id"],
                    "release_id": entity_id,
                    "artist_number": index,
                    "created_at": now,
                    "updated_at": now,
                },
            )
        return entity_id

    def _insert_track(self, now: datetime) -> UUID | None:
        releases = self.repository.sample("release", 1)
        genres = [
            row
            for row in self.repository.sample("genre", self.random.randint(1, 3))
            if row["deleted_at"] is None and row["created_at"] <= now
        ]
        if not releases or not genres:
            return None
        release = releases[0]
        if release["deleted_at"] is not None or release["created_at"] > now:
            return None
        credits = sorted(
            self.repository.rows("artist_release", release_id=release["id"]),
            key=lambda row: row["artist_number"],
        )
        if not credits:
            return None
        artists = []
        for credit in credits:
            rows = self.repository.rows("artist", id=credit["artist_id"])
            if rows and rows[0]["deleted_at"] is None and rows[0]["created_at"] <= now:
                artists.append(rows[0])
        if not artists:
            return None
        entity_id = uuid4()
        self.repository.insert(
            "track",
            {
                "id": entity_id,
                "name": self.fake.sentence(nb_words=4).rstrip("."),
                "duration_ms": self.random.randint(60_000, 480_000),
                "image_url": None,
                "is_available": True,
                "created_at": now,
                "updated_at": now,
                "deleted_at": None,
            },
        )
        for index, artist in enumerate(artists, 1):
            self.repository.insert(
                "track_artist",
                {
                    "track_id": entity_id,
                    "artist_id": artist["id"],
                    "artist_number": index,
                    "created_at": now,
                    "updated_at": now,
                },
            )
        for genre in genres:
            self.repository.insert(
                "track_genre",
                {"track_id": entity_id, "genre_id": genre["id"], "created_at": now},
            )
        self.repository.insert(
            "track_release",
            {
                "track_id": entity_id,
                "release_id": release["id"],
                "track_number": self.repository.next_track_number(release["id"]),
                "disc_number": 1,
                "created_at": now,
                "updated_at": now,
            },
        )
        return entity_id

    def _insert_installation(self, now: datetime) -> UUID | None:
        profiles = {
            (device.name, device.device_version): device.os
            for device in self.catalogs.devices
        }
        devices = self.repository.rows("device")
        self.random.shuffle(devices)
        for device in devices:
            profile = profiles.get((device["name"], device["device_version"]))
            if profile is None or device["created_at"] > now:
                continue
            releases = [
                row
                for row in self.repository.rows(
                    "app_release", platform=profile.platform
                )
                if row["released_at"] <= now and row["created_at"] <= now
            ]
            if not releases:
                continue
            app_release = self.random.choice(releases)
            entity_id = uuid4()
            self.repository.insert(
                "installation",
                {
                    "id": entity_id,
                    "device_id": device["id"],
                    "app_release_id": app_release["id"],
                    "installed_at": now,
                    "last_seen_at": None,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return entity_id
        return None

    def _update(
        self, table: str, now: datetime, excluded: Sequence[UUID]
    ) -> UUID | None:
        rows = self.repository.sample(table, excluded=excluded)
        if not rows:
            return None
        row = rows[0]
        if (
            row["created_at"] > now
            or row["updated_at"] >= now
            or row.get("deleted_at") is not None
        ):
            return None
        values = {"updated_at": now}
        if table == "installation":
            if row["installed_at"] > now or (
                row["last_seen_at"] is not None and row["last_seen_at"] > now
            ):
                return None
            values["last_seen_at"] = now
        elif table == "artist":
            values.update(
                biography=self.fake.text(max_nb_chars=450),
                is_verified=not row["is_verified"],
            )
        else:
            values.update(
                name=self.fake.sentence(nb_words=4).rstrip("."),
                is_available=not row["is_available"],
            )
        self.repository.update(table, row["id"], values)
        return row["id"]
