from src.extractors import DeviceExtractor
from src.loaders import DimDeviceLoader
from src.models.dwh import DimDevice
from src.models.oltp import Device
from src.transformers import DimDeviceTransformer

from ._pipeline import BasePipeline


class DimDevicePipeline(BasePipeline[Device, DimDevice]):
    extractor = DeviceExtractor
    transformer = DimDeviceTransformer
    loader = DimDeviceLoader
