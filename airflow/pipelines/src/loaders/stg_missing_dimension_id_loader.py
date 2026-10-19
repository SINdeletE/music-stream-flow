from src.models.dwh import StgMissingDimensionId

from .base import ClickHouseLoader


class StgMissingDimensionIdLoader(ClickHouseLoader[StgMissingDimensionId]):
    model = StgMissingDimensionId
