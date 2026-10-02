from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class DimEventType(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.dim_event_type"

    event_type_sk: int
    name: str
    loaded_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
