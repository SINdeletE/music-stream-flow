from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from .base import ClickHouseModel


@dataclass(slots=True, kw_only=True)
class EtlLoad(ClickHouseModel):
    table_name: ClassVar[str] = "music_dwh.etl_loads"
    distributed_table_name: ClassVar[str | None] = None

    load_id: UUID
    dag_id: str
    run_id: str
    task_id: str

    source_system: str
    target_table: str

    interval_start: datetime
    interval_end: datetime
    started_at: datetime
    finished_at: datetime | None = None

    state: str
    rows_read: int
    rows_written: int
    rows_rejected: int
    error_msg: str | None = None
