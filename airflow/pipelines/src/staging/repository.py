import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.loaders import StgBatchLoader, StgDatasetLoader, StgMissingDimensionIdLoader
from src.models.dwh import (
    ClickHouseModel,
    DatasetRef,
    StgBatch,
    StgDataset,
    StgMissingDimensionId,
)

from .codec import deserialize_batch, serialize_batch
from .registry import DIMENSIONS, SOURCES

RETENTION = timedelta(days=3)
logger = logging.getLogger(__name__)


Dataset = StgDataset


def validate_interval(start: datetime, end: datetime) -> None:
    if start.utcoffset() is None or end.utcoffset() is None:
        raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
    if start > end:
        raise ValueError("timestamp_start must not be after timestamp_end")


def _deduplicated_columns(model: type[ClickHouseModel]) -> tuple[str, str]:
    columns = model.column_names()
    projection = ", ".join(
        f"min({name}) AS {name}" if name == "stored_at" else name for name in columns
    )
    grouping = ", ".join(name for name in columns if name != "stored_at")
    return projection, grouping


class StagingRepository:
    def __init__(self, client: AsyncClient) -> None:
        self.client = client
        self.dataset_loader = StgDatasetLoader(client)
        self.batch_loader = StgBatchLoader(client)
        self.missing_id_loader = StgMissingDimensionIdLoader(client)

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
        staged_batch = StgBatch(
            dataset_id=dataset.dataset_id,
            batch_number=dataset.batch_count,
            payload=payload,
            row_count=len(batch),
            next_offset=next_offset,
            stored_at=stored_at,
        )
        await self.batch_loader.load_batch([staged_batch])
        dataset.batch_count += 1
        dataset.row_count += len(batch)
        if next_offset is not None:
            dataset.next_offset = next_offset

    async def write_missing(
        self, dataset: Dataset, missing: dict[str, set[UUID]]
    ) -> None:
        rows: list[StgMissingDimensionId] = []
        for dimension, identifiers in missing.items():
            if dimension not in DIMENSIONS:
                raise ValueError(f"Unknown dimension: {dimension}")
            if identifiers:
                stored_at = self._stored_at(dataset)
                rows.extend(
                    StgMissingDimensionId(
                        dataset_id=dataset.dataset_id,
                        dimension=dimension,
                        entity_id=identifier,
                        stored_at=stored_at,
                    )
                    for identifier in sorted(identifiers)
                )
        if rows:
            await self.missing_id_loader.load_batch(rows)

    async def missing_ids(self, dataset: Dataset) -> dict[str, set[UUID]]:
        self._ensure_live(dataset)
        projection, grouping = _deduplicated_columns(StgMissingDimensionId)
        result = await self.client.query(
            f"""SELECT {projection} FROM {StgMissingDimensionId.table_to_insert()}
                WHERE dataset_id = {{dataset_id:UUID}} GROUP BY {grouping}""",
            parameters={"dataset_id": dataset.dataset_id},
            tz_mode="aware",
        )
        missing = {name: set() for name in DIMENSIONS}
        for values in result.named_results():
            row = StgMissingDimensionId(**values)
            if row.dimension not in missing:
                raise RuntimeError(f"Unknown staged dimension: {row.dimension}")
            missing[row.dimension].add(row.entity_id)
        return missing

    async def complete(self, dataset: Dataset) -> DatasetRef:
        self._ensure_live(dataset)
        missing = await self.missing_ids(dataset)
        dataset.dimension_counts = {name: len(ids) for name, ids in missing.items()}
        await self.dataset_loader.load_batch([dataset])
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
            f"SELECT {', '.join(StgDataset.column_names())} FROM {StgDataset.table_to_insert()} "
            "WHERE dataset_id = {dataset_id:UUID}",
            parameters={"dataset_id": UUID(str(dataset_id))},
            tz_mode="aware",
        )
        rows = [StgDataset(**values) for values in result.named_results()]
        if not rows:
            raise LookupError(f"Staging dataset is not complete: {dataset_id}")
        if any(row != rows[0] for row in rows):
            raise RuntimeError("Conflicting staging metadata")
        dataset = rows[0]
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
            f"""SELECT batch_number, any(row_count) AS batch_row_count,
                       uniqExact(tuple(payload, row_count, next_offset)) AS variant_count,
                       any(next_offset) AS batch_next_offset
                FROM {StgBatch.table_to_insert()} WHERE dataset_id = {{dataset_id:UUID}}
                GROUP BY batch_number ORDER BY batch_number""",
            parameters={"dataset_id": dataset.dataset_id},
        )
        rows = list(result.named_results())
        if (
            len(rows) != dataset.batch_count
            or sum(row["batch_row_count"] for row in rows) != dataset.row_count
            or any(
                number != row["batch_number"] or row["variant_count"] != 1
                for number, row in enumerate(rows)
            )
        ):
            raise RuntimeError("Staging dataset has incomplete or conflicting batches")
        if dataset.source == "fct_events" and rows:
            previous = dataset.first_offset
            for row in rows:
                if (
                    previous is None
                    or row["batch_next_offset"] is None
                    or row["batch_next_offset"] <= previous
                ):
                    raise RuntimeError("Invalid staged Kafka offsets")
                previous = row["batch_next_offset"]
            if previous != dataset.next_offset:
                raise RuntimeError("Staged Kafka end offset does not match")

    async def read_batches(
        self, dataset: Dataset
    ) -> AsyncGenerator[tuple[list, int | None], None]:
        await self.validate(dataset)
        for number in range(dataset.batch_count):
            self._ensure_live(dataset)
            projection, grouping = _deduplicated_columns(StgBatch)
            result = await self.client.query(
                f"""SELECT {projection} FROM {StgBatch.table_to_insert()}
                    WHERE dataset_id = {{dataset_id:UUID}} AND batch_number = {{number:UInt64}}
                    GROUP BY {grouping}""",
                parameters={"dataset_id": dataset.dataset_id, "number": number},
                tz_mode="aware",
            )
            rows = [StgBatch(**values) for values in result.named_results()]
            if len(rows) != 1:
                raise RuntimeError("Staging batch is missing or conflicting")
            staged_batch = rows[0]
            batch = deserialize_batch(dataset.source, staged_batch.payload)
            if len(batch) != staged_batch.row_count:
                raise RuntimeError("Staging batch row count does not match its payload")
            yield batch, staged_batch.next_offset
