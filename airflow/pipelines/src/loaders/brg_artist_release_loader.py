from src.models.dwh import BrgArtistRelease

from .base import ClickHouseLoader


class BrgArtistReleaseLoader(ClickHouseLoader[BrgArtistRelease]):
    model = BrgArtistRelease
