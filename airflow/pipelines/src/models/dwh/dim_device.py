from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimDevice(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_device"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_device_all"

    device_sk: int
    device_id: UUID
    user_id: UUID
    name: str
    device_version: str
    loaded_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
