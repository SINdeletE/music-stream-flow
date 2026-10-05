from src.extractors import ArtistReleaseExtractor
from src.loaders import BrgArtistReleaseLoader
from src.models.dwh import BrgArtistRelease
from src.models.oltp import ArtistRelease
from src.transformers import BrgArtistReleaseTransformer

from ._pipeline import BasePipeline


class BrgArtistReleasePipeline(BasePipeline[ArtistRelease, BrgArtistRelease]):
    extractor = ArtistReleaseExtractor
    transformer = BrgArtistReleaseTransformer
    loader = BrgArtistReleaseLoader
