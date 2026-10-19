from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar, TypedDict
from uuid import UUID, uuid7

from .base import ClickHouseModel


class DatasetRef(TypedDict):
    dataset_id: str


@dataclass(slots=True, kw_only=True)
class StgDataset(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.stg_datasets"
    distributed_table_name: ClassVar[str | None] = "music_dwh.stg_datasets_all"

    source: str
    dag_id: str
    run_id: str
    task_id: str
    interval_start: datetime
    interval_end: datetime
    dataset_id: UUID = field(default_factory=uuid7)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    batch_count: int = 0
    row_count: int = 0
    dimension_counts: dict[str, int] = field(default_factory=dict)
    topic: str | None = None
    partition: int | None = None
    committed_offset: int | None = None
    first_offset: int | None = None
    next_offset: int | None = None
    data_expires_at: datetime | None = None

    def reference(self) -> DatasetRef:
        return {"dataset_id": str(self.dataset_id)}
