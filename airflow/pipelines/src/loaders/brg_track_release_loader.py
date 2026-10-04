from src.models.dwh import BrgTrackRelease

from .base import ClickHouseLoader


class BrgTrackReleaseLoader(ClickHouseLoader[BrgTrackRelease]):
    model = BrgTrackRelease
