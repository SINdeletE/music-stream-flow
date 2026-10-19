from src.models.dwh import StgDataset

from .base import ClickHouseLoader


class StgDatasetLoader(ClickHouseLoader[StgDataset]):
    model = StgDataset
