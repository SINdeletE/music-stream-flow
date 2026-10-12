from dataclasses import dataclass
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class BrgDeviceAppRelease(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.brg_device_app_release"
    distributed_table_name: ClassVar[str | None] = (
        "music_dwh.brg_device_app_release_all"
    )

    device_sk: int
    app_release_sk: int
    load_id: UUID
