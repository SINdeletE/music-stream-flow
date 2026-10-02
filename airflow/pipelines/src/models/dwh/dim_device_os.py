from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimDeviceOs(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_device_os"
    distributed_table_name: ClassVar[str | None] = "music_dwh.dim_device_os_all"

    device_os_sk: int | None = None
    device_os_id: UUID
    name: str
    device_os_version: str
    valid_from: datetime
    load_id: UUID
    source_created_at: datetime
