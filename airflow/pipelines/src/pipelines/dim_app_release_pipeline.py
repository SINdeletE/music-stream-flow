from src.extractors import AppReleaseExtractor
from src.loaders import DimAppReleaseLoader
from src.models.dwh import DimAppRelease
from src.models.oltp import AppRelease
from src.transformers import DimAppReleaseTransformer

from ._pipeline import BasePipeline


class DimAppReleasePipeline(BasePipeline[AppRelease, DimAppRelease]):
    extractor = AppReleaseExtractor
    transformer = DimAppReleaseTransformer
    loader = DimAppReleaseLoader
