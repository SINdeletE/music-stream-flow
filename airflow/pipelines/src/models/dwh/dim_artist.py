from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimArtist(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_artist"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_artist_all"

    artist_sk: int | None = None
    artist_id: UUID
    nickname: str
    name: str | None = None
    middle_name: str | None = None
    last_name: str | None = None
    country_code: str | None = None
    biography: str | None = None
    is_verified: bool
    load_id: UUID
    source_created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
