"""SQLAlchemy models matching sql/music_oltp/ddl."""

from .app_release import AppRelease
from .artist import Artist
from .artist_release import ArtistRelease
from .base import Base
from .device import Device
from .device_os_history import DeviceOsHistory
from .genre import Genre
from .installation import Installation
from .release import Release
from .track import Track
from .track_artist import TrackArtist
from .track_genre import TrackGenre
from .track_release import TrackRelease

__all__ = [
    "Base",
    "AppRelease",
    "Artist",
    "ArtistRelease",
    "Device",
    "DeviceOsHistory",
    "Genre",
    "Installation",
    "Release",
    "Track",
    "TrackArtist",
    "TrackGenre",
    "TrackRelease",
]
