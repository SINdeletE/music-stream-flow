from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class StgBatch(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.stg_batches"
    distributed_table_name: ClassVar[str | None] = "music_dwh.stg_batches_all"

    dataset_id: UUID
    batch_number: int
    payload: str
    row_count: int
    next_offset: int | None = None
    stored_at: datetime
