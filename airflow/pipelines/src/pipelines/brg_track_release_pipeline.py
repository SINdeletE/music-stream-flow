from src.extractors import TrackReleaseExtractor
from src.loaders import BrgTrackReleaseLoader
from src.models.dwh import BrgTrackRelease
from src.models.oltp import TrackRelease
from src.transformers import BrgTrackReleaseTransformer

from ._pipeline import BasePipeline


class BrgTrackReleasePipeline(BasePipeline[TrackRelease, BrgTrackRelease]):
    extractor = TrackReleaseExtractor
    transformer = BrgTrackReleaseTransformer
    loader = BrgTrackReleaseLoader
