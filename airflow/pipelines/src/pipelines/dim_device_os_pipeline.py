from src.extractors import DeviceOsExtractor
from src.loaders import DimDeviceOsLoader
from src.models.dwh import DimDeviceOs
from src.models.oltp import DeviceOsHistory
from src.transformers import DimDeviceOsTransformer

from ._pipeline import BasePipeline


class DimDeviceOsPipeline(BasePipeline[DeviceOsHistory, DimDeviceOs]):
    extractor = DeviceOsExtractor
    transformer = DimDeviceOsTransformer
    loader = DimDeviceOsLoader
