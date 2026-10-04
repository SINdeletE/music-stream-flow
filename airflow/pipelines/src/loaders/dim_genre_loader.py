from src.models.dwh import DimGenre

from .base import ClickHouseLoader


class DimGenreLoader(ClickHouseLoader[DimGenre]):
    model = DimGenre
    generated_columns = ("genre_sk",)
