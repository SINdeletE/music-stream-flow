from src.extractors import TrackExtractor
from src.loaders import DimTrackLoader
from src.models.dwh import DimTrack
from src.models.oltp import Track
from src.transformers import DimTrackTransformer

from ._pipeline import BasePipeline


class DimTrackPipeline(BasePipeline[Track, DimTrack]):
    extractor = TrackExtractor
    transformer = DimTrackTransformer
    loader = DimTrackLoader
