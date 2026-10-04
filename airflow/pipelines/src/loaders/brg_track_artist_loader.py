from src.models.dwh import BrgTrackArtist

from .base import ClickHouseLoader


class BrgTrackArtistLoader(ClickHouseLoader[BrgTrackArtist]):
    model = BrgTrackArtist
