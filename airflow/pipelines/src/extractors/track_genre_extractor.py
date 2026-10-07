from src.models.oltp import TrackGenre

from .oltp_extractor import OltpExtractor


class TrackGenreExtractor(OltpExtractor[TrackGenre]):
    model = TrackGenre
    timestamp_column = TrackGenre.created_at
    id_columns = (TrackGenre.track_id, TrackGenre.genre_id)
