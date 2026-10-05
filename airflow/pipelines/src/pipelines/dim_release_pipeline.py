from src.extractors import ReleaseExtractor
from src.loaders import DimReleaseLoader
from src.models.dwh import DimRelease
from src.models.oltp import Release
from src.transformers import DimReleaseTransformer

from ._pipeline import BasePipeline


class DimReleasePipeline(BasePipeline[Release, DimRelease]):
    extractor = ReleaseExtractor
    transformer = DimReleaseTransformer
    loader = DimReleaseLoader
