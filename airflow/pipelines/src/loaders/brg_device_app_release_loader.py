from src.models.dwh import BrgDeviceAppRelease

from .base import ClickHouseLoader


class BrgDeviceAppReleaseLoader(ClickHouseLoader[BrgDeviceAppRelease]):
    model = BrgDeviceAppRelease
