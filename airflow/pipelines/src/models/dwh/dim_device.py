from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimDevice(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_device"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_device_all"

    device_sk: int | None = None
    device_id: UUID
    name: str
    device_version: str
    load_id: UUID
    source_created_at: datetime
    updated_at: datetime
