from dataclasses import dataclass
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class BrgArtistRelease(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.brg_artist_release"
    distributed_table_name: ClassVar[str | None] = "music_dwh.brg_artist_release_all"

    artist_sk: int
    release_sk: int
    artist_number: int
    load_id: UUID
