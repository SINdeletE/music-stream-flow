from src.models.dwh import DimRelease

from .base import ClickHouseLoader


class DimReleaseLoader(ClickHouseLoader[DimRelease]):
    model = DimRelease
    generated_columns = ("release_sk",)
