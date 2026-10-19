from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class StgMissingDimensionId(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.stg_missing_dimension_ids"
    distributed_table_name: ClassVar[str | None] = (
        "music_dwh.stg_missing_dimension_ids_all"
    )

    dataset_id: UUID
    dimension: str
    entity_id: UUID
    stored_at: datetime
