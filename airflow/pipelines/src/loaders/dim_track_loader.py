from src.models.dwh import DimTrack

from .base import ClickHouseLoader


class DimTrackLoader(ClickHouseLoader[DimTrack]):
    model = DimTrack
    generated_columns = ("track_sk",)
