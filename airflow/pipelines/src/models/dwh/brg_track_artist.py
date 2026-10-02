from dataclasses import dataclass
from typing import ClassVar

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class BrgTrackArtist(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.brg_track_artist"
    distributed_table_name: ClassVar[str | None] = "music_dwh.brg_track_artist_all"

    track_sk: int
    artist_sk: int
    artist_number: int
