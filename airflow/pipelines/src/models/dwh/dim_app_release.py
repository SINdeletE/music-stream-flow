from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimAppRelease(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_app_release"

    app_release_sk: int | None = None
    app_release_id: UUID
    platform: str
    app_release_version: str
    build: str
    released_at: datetime
    load_id: UUID
    source_created_at: datetime
