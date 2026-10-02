from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimAppRelease(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_app_release"

    app_release_sk: int
    app_release_id: UUID
    platform: str
    app_release_version: str
    build: str
    released_at: datetime
    loaded_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
