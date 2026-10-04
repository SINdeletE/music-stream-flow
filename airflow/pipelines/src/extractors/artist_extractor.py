from src.models.oltp import Artist

from .base import OltpExtractor


class ArtistExtractor(OltpExtractor[Artist]):
    model = Artist
    timestamp_column = Artist.updated_at
    id_columns = (Artist.id,)
