from src.extractors import TrackGenreExtractor
from src.loaders import BrgTrackGenreLoader
from src.models.dwh import BrgTrackGenre
from src.models.oltp import TrackGenre
from src.transformers import BrgTrackGenreTransformer

from ._pipeline import BasePipeline


class BrgTrackGenrePipeline(BasePipeline[TrackGenre, BrgTrackGenre]):
    extractor = TrackGenreExtractor
    transformer = BrgTrackGenreTransformer
    loader = BrgTrackGenreLoader
