from src.extractors import ArtistExtractor
from src.loaders import DimArtistLoader
from src.models.dwh import DimArtist
from src.models.oltp import Artist
from src.transformers import DimArtistTransformer

from ._pipeline import BasePipeline


class DimArtistPipeline(BasePipeline[Artist, DimArtist]):
    extractor = ArtistExtractor
    transformer = DimArtistTransformer
    loader = DimArtistLoader
