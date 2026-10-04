from src.models.dwh import DimDeviceOs

from .base import ClickHouseLoader


class DimDeviceOsLoader(ClickHouseLoader[DimDeviceOs]):
    model = DimDeviceOs
    generated_columns = ("device_os_sk",)
