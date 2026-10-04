from src.models.dwh import DimArtist

from .base import ClickHouseLoader


class DimArtistLoader(ClickHouseLoader[DimArtist]):
    model = DimArtist
    generated_columns = ("artist_sk",)
