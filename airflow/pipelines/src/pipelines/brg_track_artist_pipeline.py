from src.extractors import TrackArtistExtractor
from src.loaders import BrgTrackArtistLoader
from src.models.dwh import BrgTrackArtist
from src.models.oltp import TrackArtist
from src.transformers import BrgTrackArtistTransformer

from ._pipeline import BasePipeline


class BrgTrackArtistPipeline(BasePipeline[TrackArtist, BrgTrackArtist]):
    extractor = TrackArtistExtractor
    transformer = BrgTrackArtistTransformer
    loader = BrgTrackArtistLoader
