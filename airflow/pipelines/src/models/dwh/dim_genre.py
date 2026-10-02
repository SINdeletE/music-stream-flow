from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimGenre(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_genre"

    genre_sk: int | None = None
    genre_id: UUID
    name: str
    load_id: UUID
    source_created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
