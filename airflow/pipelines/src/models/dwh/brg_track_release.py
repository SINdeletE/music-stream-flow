from dataclasses import dataclass
from typing import ClassVar

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class BrgTrackRelease(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.brg_track_release"
    distributed_table_name: ClassVar[str | None] = "music_dwh.brg_track_release_all"

    track_sk: int
    release_sk: int
    track_number: int
    disc_number: int | None = None
