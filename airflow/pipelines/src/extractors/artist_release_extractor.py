from src.models.oltp import ArtistRelease

from .oltp_extractor import OltpExtractor


class ArtistReleaseExtractor(OltpExtractor[ArtistRelease]):
    model = ArtistRelease
    timestamp_column = ArtistRelease.updated_at
    id_columns = (ArtistRelease.artist_id, ArtistRelease.release_id)
