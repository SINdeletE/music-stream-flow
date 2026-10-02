from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimTrack(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_track"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_track_all"

    track_sk: int | None = None
    track_id: UUID
    name: str
    duration_ms: int
    is_available: bool = False
    load_id: UUID
    source_created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
