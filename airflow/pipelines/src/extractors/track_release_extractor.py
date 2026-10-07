from src.models.oltp import TrackRelease

from .oltp_extractor import OltpExtractor


class TrackReleaseExtractor(OltpExtractor[TrackRelease]):
    model = TrackRelease
    timestamp_column = TrackRelease.updated_at
    id_columns = (TrackRelease.track_id, TrackRelease.release_id)
