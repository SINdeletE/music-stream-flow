from src.models.dwh import DimDevice

from .base import ClickHouseLoader


class DimDeviceLoader(ClickHouseLoader[DimDevice]):
    model = DimDevice
    generated_columns = ("device_sk",)
