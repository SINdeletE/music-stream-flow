import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime, timedelta
from typing import TypedDict
from uuid import UUID, uuid7

from clickhouse_connect.driver.asyncclient import AsyncClient

from .codec import deserialize_batch, serialize_batch
from .registry import DIMENSIONS, SOURCES

RETENTION = timedelta(days=3)
logger = logging.getLogger(__name__)


class DatasetRef(TypedDict):
    dataset_id: str


def validate_interval(start: datetime, end: datetime) -> None:
    if start.utcoffset() is None or end.utcoffset() is None:
        raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
    if start > end:
        raise ValueError("timestamp_start must not be after timestamp_end")


@dataclass(kw_only=True)
class Dataset:
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


class StagingRepository:
    def __init__(self, client: AsyncClient) -> None:
        self.client = client

    @staticmethod
    def _ensure_live(dataset: Dataset) -> None:
        if (
            dataset.data_expires_at is not None
            and datetime.now(UTC) >= dataset.data_expires_at
        ):
            raise RuntimeError(f"Staging dataset expired: {dataset.dataset_id}")

    @staticmethod
    def _stored_at(dataset: Dataset) -> datetime:
        stored_at = datetime.now(UTC).replace(microsecond=0)
        expires_at = stored_at + RETENTION
        if dataset.data_expires_at is None or expires_at < dataset.data_expires_at:
            dataset.data_expires_at = expires_at
        return stored_at

    async def write_batch(
        self, dataset: Dataset, batch: list, *, next_offset: int | None = None
    ) -> None:
        if not batch:
            return
        payload = serialize_batch(dataset.source, batch)
        stored_at = self._stored_at(dataset)
        await self.client.insert(
            "music_dwh.stg_batches_all",
            [
                (
                    dataset.dataset_id,
                    dataset.batch_count,
                    payload,
                    len(batch),
                    next_offset,
                    stored_at,
                )
            ],
            column_names=(
                "dataset_id",
                "batch_number",
                "payload",
                "row_count",
                "next_offset",
                "stored_at",
            ),
        )
        dataset.batch_count += 1
        dataset.row_count += len(batch)
        if next_offset is not None:
            dataset.next_offset = next_offset

    async def write_missing(
        self, dataset: Dataset, missing: dict[str, set[UUID]]
    ) -> None:
        rows = []
        for dimension, identifiers in missing.items():
            if dimension not in DIMENSIONS:
                raise ValueError(f"Unknown dimension: {dimension}")
            if identifiers:
                stored_at = self._stored_at(dataset)
                rows.extend(
                    (dataset.dataset_id, dimension, identifier, stored_at)
                    for identifier in sorted(identifiers)
                )
        if rows:
            await self.client.insert(
                "music_dwh.stg_missing_dimension_ids_all",
                rows,
                column_names=("dataset_id", "dimension", "entity_id", "stored_at"),
            )

    async def missing_ids(self, dataset: Dataset) -> dict[str, set[UUID]]:
        self._ensure_live(dataset)
        result = await self.client.query(
            """SELECT dimension, entity_id FROM music_dwh.stg_missing_dimension_ids_all
               WHERE dataset_id = {dataset_id:UUID} GROUP BY dimension, entity_id""",
            parameters={"dataset_id": dataset.dataset_id},
        )
        missing = {name: set() for name in DIMENSIONS}
        for dimension, identifier in result.result_rows:
            if dimension not in missing:
                raise RuntimeError(f"Unknown staged dimension: {dimension}")
            missing[dimension].add(identifier)
        return missing

    async def complete(self, dataset: Dataset) -> DatasetRef:
        self._ensure_live(dataset)
        missing = await self.missing_ids(dataset)
        dataset.dimension_counts = {name: len(ids) for name, ids in missing.items()}
        columns = tuple(item.name for item in fields(Dataset))
        await self.client.insert(
            "music_dwh.stg_datasets_all",
            [tuple(getattr(dataset, name) for name in columns)],
            column_names=columns,
        )
        logger.info(
            "Staging dataset ready: dataset_id=%s source=%s batches=%s rows=%s",
            dataset.dataset_id,
            dataset.source,
            dataset.batch_count,
            dataset.row_count,
        )
        return dataset.reference()

    async def get(
        self,
        dataset_id: UUID | str,
        *,
        dag_id: str,
        run_id: str,
        timestamp_start: datetime,
        timestamp_end: datetime,
        source: str | None = None,
    ) -> Dataset:
        validate_interval(timestamp_start, timestamp_end)
        result = await self.client.query(
            "SELECT * FROM music_dwh.stg_datasets_all WHERE dataset_id = {dataset_id:UUID}",
            parameters={"dataset_id": UUID(str(dataset_id))},
            tz_mode="aware",
        )
        rows = list(result.named_results())
        if not rows:
            raise LookupError(f"Staging dataset is not complete: {dataset_id}")
        if any(row != rows[0] for row in rows):
            raise RuntimeError("Conflicting staging metadata")
        dataset = Dataset(**rows[0])
        if (
            dataset.dag_id != dag_id
            or dataset.run_id != run_id
            or dataset.interval_start != timestamp_start
            or dataset.interval_end != timestamp_end
            or (source is not None and dataset.source != source)
        ):
            raise ValueError(
                "Staging dataset does not match the requested run, interval or source"
            )
        if dataset.source not in (*SOURCES, "collect"):
            raise ValueError("Unknown staging source")
        await self.validate(dataset)
        return dataset

    async def validate(self, dataset: Dataset) -> None:
        self._ensure_live(dataset)
        missing = await self.missing_ids(dataset)
        if {
            name: len(ids) for name, ids in missing.items()
        } != dataset.dimension_counts:
            raise RuntimeError("Staging dataset has incomplete missing IDs")
        result = await self.client.query(
            """SELECT batch_number, any(row_count), uniqExact(tuple(payload, row_count, next_offset)), any(next_offset)
               FROM music_dwh.stg_batches_all WHERE dataset_id = {dataset_id:UUID}
               GROUP BY batch_number ORDER BY batch_number""",
            parameters={"dataset_id": dataset.dataset_id},
        )
        rows = result.result_rows
        if (
            len(rows) != dataset.batch_count
            or sum(row[1] for row in rows) != dataset.row_count
            or any(number != row[0] or row[2] != 1 for number, row in enumerate(rows))
        ):
            raise RuntimeError("Staging dataset has incomplete or conflicting batches")
        if dataset.source == "fct_events" and rows:
            previous = dataset.first_offset
            for row in rows:
                if previous is None or row[3] is None or row[3] <= previous:
                    raise RuntimeError("Invalid staged Kafka offsets")
                previous = row[3]
            if previous != dataset.next_offset:
                raise RuntimeError("Staged Kafka end offset does not match")

    async def read_batches(
        self, dataset: Dataset
    ) -> AsyncGenerator[tuple[list, int | None], None]:
        await self.validate(dataset)
        for number in range(dataset.batch_count):
            self._ensure_live(dataset)
            result = await self.client.query(
                """SELECT payload, row_count, next_offset FROM music_dwh.stg_batches_all
                   WHERE dataset_id = {dataset_id:UUID} AND batch_number = {number:UInt64}
                   GROUP BY payload, row_count, next_offset""",
                parameters={"dataset_id": dataset.dataset_id, "number": number},
            )
            if len(result.result_rows) != 1:
                raise RuntimeError("Staging batch is missing or conflicting")
            payload, count, next_offset = result.result_rows[0]
            batch = deserialize_batch(dataset.source, payload)
            if len(batch) != count:
                raise RuntimeError("Staging batch row count does not match its payload")
            yield batch, next_offset
