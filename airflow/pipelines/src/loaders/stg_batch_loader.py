from src.models.dwh import StgBatch

from .base import ClickHouseLoader


class StgBatchLoader(ClickHouseLoader[StgBatch]):
    model = StgBatch
