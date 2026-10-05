from src.extractors import InstallationExtractor
from src.loaders import BrgDeviceAppReleaseLoader
from src.models.dwh import BrgDeviceAppRelease
from src.models.oltp import Installation
from src.transformers import BrgDeviceAppReleaseTransformer

from ._pipeline import BasePipeline


class BrgDeviceAppReleasePipeline(BasePipeline[Installation, BrgDeviceAppRelease]):
    extractor = InstallationExtractor
    transformer = BrgDeviceAppReleaseTransformer
    loader = BrgDeviceAppReleaseLoader
