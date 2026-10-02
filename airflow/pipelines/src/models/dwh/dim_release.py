from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimRelease(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_release"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_release_all"

    release_sk: int | None = None
    release_id: UUID
    name: str
    is_available: bool = False
    released_at: datetime
    load_id: UUID
    source_created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
