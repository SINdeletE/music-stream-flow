from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimTrack(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_track"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_track_all"

    track_sk: int
    track_id: UUID
    name: str
    duration_ms: int
    is_available: bool = False
    track_created_at: datetime
    track_deleted_at: datetime | None = None
    loaded_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
