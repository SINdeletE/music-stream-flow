from dataclasses import dataclass

from src.extractors import (
    ArtistReleaseExtractor,
    InstallationExtractor,
    TrackArtistExtractor,
    TrackGenreExtractor,
    TrackReleaseExtractor,
)
from src.models.kafka import AnalyticsEvent
from src.models.oltp import (
    ArtistRelease,
    Installation,
    TrackArtist,
    TrackGenre,
    TrackRelease,
)


@dataclass(frozen=True)
class Dimension:
    name: str
    table: str
    version: str = "updated_at"


DIMENSIONS = {
    "device": Dimension("device", "music_dwh.dim_device_all"),
    "device_os": Dimension("device_os", "music_dwh.dim_device_os_all", "valid_from"),
    "app_release": Dimension(
        "app_release", "music_dwh.dim_app_release", "source_created_at"
    ),
    "track": Dimension("track", "music_dwh.dim_track_all"),
    "artist": Dimension("artist", "music_dwh.dim_artist_all"),
    "genre": Dimension("genre", "music_dwh.dim_genre"),
    "release": Dimension("release", "music_dwh.dim_release_all"),
}


@dataclass(frozen=True)
class Source:
    model: type
    dimensions: tuple[str, ...]
    extractor: type | None = None


SOURCES = {
    "brg_device_app_release": Source(
        Installation, ("device", "app_release"), InstallationExtractor
    ),
    "brg_track_genre": Source(TrackGenre, ("track", "genre"), TrackGenreExtractor),
    "brg_track_artist": Source(TrackArtist, ("track", "artist"), TrackArtistExtractor),
    "brg_track_release": Source(
        TrackRelease, ("track", "release"), TrackReleaseExtractor
    ),
    "brg_artist_release": Source(
        ArtistRelease, ("artist", "release"), ArtistReleaseExtractor
    ),
    "fct_events": Source(
        AnalyticsEvent, ("device", "device_os", "app_release", "track")
    ),
}

MODEL_DIMENSIONS = {
    "device": "device",
    "device_os_history": "device_os",
    "app_release": "app_release",
    "track": "track",
    "artist": "artist",
    "genre": "genre",
    "release": "release",
}
