from src.models.dwh import BrgTrackGenre

from .base import ClickHouseLoader


class BrgTrackGenreLoader(ClickHouseLoader[BrgTrackGenre]):
    model = BrgTrackGenre
