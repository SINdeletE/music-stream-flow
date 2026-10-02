from dataclasses import dataclass
from typing import ClassVar

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class BrgTrackGenre(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.brg_track_genre"
    distributed_table_name: ClassVar[str | None] = "music_dwh.brg_track_genre_all"

    track_sk: int
    genre_sk: int
