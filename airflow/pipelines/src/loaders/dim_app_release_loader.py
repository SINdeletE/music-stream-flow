from src.models.dwh import DimAppRelease

from .base import ClickHouseLoader


class DimAppReleaseLoader(ClickHouseLoader[DimAppRelease]):
    model = DimAppRelease
    generated_columns = ("app_release_sk",)
