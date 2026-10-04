from src.models.oltp import TrackArtist

from .base import OltpExtractor


class TrackArtistExtractor(OltpExtractor[TrackArtist]):
    model = TrackArtist
    timestamp_column = TrackArtist.updated_at
    id_columns = (TrackArtist.track_id, TrackArtist.artist_id)
