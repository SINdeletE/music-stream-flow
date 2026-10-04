from src.models.oltp import Track

from .base import OltpExtractor


class TrackExtractor(OltpExtractor[Track]):
    model = Track
    timestamp_column = Track.updated_at
    id_columns = (Track.id,)
