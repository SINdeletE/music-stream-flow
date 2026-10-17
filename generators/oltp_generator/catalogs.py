import json
from dataclasses import dataclass
from pathlib import Path
from typing import Self

CATALOG_DIRECTORY = Path(__file__).resolve().parents[1] / "catalogs"


@dataclass(frozen=True)
class OsProfile:
    name: str
    platform: str
    versions: tuple[str, ...]


@dataclass(frozen=True)
class Device:
    name: str
    device_version: str
    os: OsProfile


@dataclass(frozen=True)
class Catalogs:
    devices: tuple[Device, ...]
    genres: tuple[str, ...]

    @classmethod
    def load(cls, directory: Path = CATALOG_DIRECTORY) -> Self:
        def read(name: str):
            return json.loads((directory / name).read_text(encoding="utf-8"))

        profiles = {
            key: OsProfile(value["name"], value["platform"], tuple(value["versions"]))
            for key, value in read("operating_systems.json").items()
        }
        devices = tuple(
            Device(row["name"], row["device_version"], profiles[row["os_profile"]])
            for row in read("devices.json")
        )
        genres = tuple(read("genres.json"))
        if not devices or not genres:
            raise ValueError("Catalogs must not be empty")
        if len({(device.name, device.device_version) for device in devices}) != len(
            devices
        ):
            raise ValueError("Duplicate catalog devices")
        if len(set(genres)) != len(genres):
            raise ValueError("Duplicate catalog genres")
        if any(not name or len(name) > 64 for name in genres):
            raise ValueError("Invalid catalog genre")
        for device in devices:
            if (
                not device.name
                or len(device.name) > 128
                or not device.device_version
                or len(device.device_version) > 16
            ):
                raise ValueError("Invalid catalog device")
            if not device.os.versions or len(set(device.os.versions)) != len(
                device.os.versions
            ):
                raise ValueError("Invalid ordered OS versions")
            if (
                len(device.os.name) > 32
                or len(device.os.platform) > 16
                or any(
                    not version or len(version) > 32 for version in device.os.versions
                )
            ):
                raise ValueError("Invalid OS profile")
        return cls(devices, genres)
