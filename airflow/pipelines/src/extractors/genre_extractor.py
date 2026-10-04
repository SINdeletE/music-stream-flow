from src.models.oltp import Genre

from .base import OltpExtractor


class GenreExtractor(OltpExtractor[Genre]):
    model = Genre
    timestamp_column = Genre.updated_at
    id_columns = (Genre.id,)
