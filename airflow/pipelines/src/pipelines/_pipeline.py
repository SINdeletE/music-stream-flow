import logging
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID, uuid7

from src.connections.dwh_connector import DwhConnector
from src.connections.oltp_connector import OltpConnector
from src.extractors.oltp_extractor import OltpExtractor
from src.loaders import ClickHouseLoader, EtlLoadLoader
from src.models.dwh import ClickHouseModel, EtlLoad
from src.models.oltp import Base
from src.settings import DwhSettings, OltpSettings
from src.staging import StagingRepository
from src.staging.registry import MODEL_DIMENSIONS
from src.transformers import BatchTransformer

logger = logging.getLogger(__name__)
SourceT = TypeVar("SourceT", bound=Base)
TargetT = TypeVar("TargetT", bound=ClickHouseModel)


class BasePipeline(Generic[SourceT, TargetT]):
    extractor: type[OltpExtractor[SourceT]]
    transformer: type[BatchTransformer[SourceT, TargetT]]
    loader: type[ClickHouseLoader[TargetT]]

    @classmethod
    async def run(
        cls,
        *,
        timestamp_start: datetime,
        timestamp_end: datetime,
        dag_id: str,
        run_id: str,
        task_id: str,
        oltp_settings: OltpSettings | None = None,
        dwh_settings: DwhSettings | None = None,
        missing_ids_dataset_id: UUID | str | None = None,
        staging_dataset_id: UUID | str | None = None,
    ) -> EtlLoad:
        if timestamp_start.utcoffset() is None or timestamp_end.utcoffset() is None:
            raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
        if timestamp_start > timestamp_end:
            raise ValueError("timestamp_start must not be after timestamp_end")
        if missing_ids_dataset_id is not None and staging_dataset_id is not None:
            raise ValueError("Cannot combine missing IDs and a source staging dataset")
        if oltp_settings is None and staging_dataset_id is None:
            oltp_settings = OltpSettings()  # type: ignore[call-arg]
        if dwh_settings is None:
            dwh_settings = DwhSettings()  # type: ignore[call-arg]

        async with DwhConnector(dwh_settings) as client:
            audit_loader = EtlLoadLoader(client)
            audit = EtlLoad(
                load_id=uuid7(),
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                source_system="music_oltp",
                target_table=cls.loader.model.table_to_insert(),  # type: ignore[call-arg]
                interval_start=timestamp_start,
                interval_end=timestamp_end,
                started_at=datetime.now(UTC),
                state="running",
                rows_read=0,
                rows_written=0,
                rows_rejected=0,
            )

            try:
                await audit_loader.load_batch([audit])
                loader = cls.loader(client)
                async with aclosing(
                    cls._source_batches(
                        client,
                        timestamp_start,
                        timestamp_end,
                        dag_id,
                        run_id,
                        oltp_settings,
                        missing_ids_dataset_id,
                        staging_dataset_id,
                    )
                ) as batches:
                    async for source_batch in batches:
                        audit.rows_read += len(source_batch)
                        logger.info(
                            "Extractor read %s records for load_id=%s",
                            len(source_batch),
                            audit.load_id,
                        )

                        dwh_batch = await cls.transformer.transform(
                            source_batch, client, load_id=audit.load_id
                        )
                        rows_written = await loader.load_batch(dwh_batch)

                        audit.rows_written += rows_written
                        logger.info(
                            "Loader loaded %s records for load_id=%s",
                            len(source_batch),
                            audit.load_id,
                        )

                audit.state = "success"
                audit.finished_at = datetime.now(UTC)
                await audit_loader.load_batch([audit])
            except Exception as exc:
                audit.state = "failed"
                audit.finished_at = datetime.now(UTC)
                audit.error_msg = f"{type(exc).__name__}: {exc}"

                try:
                    await audit_loader.load_batch([audit])
                except Exception:
                    logger.exception(
                        "Failed to write audit for load_id=%s", audit.load_id
                    )
                raise

        return audit

    @classmethod
    async def _source_batches(
        cls,
        client,
        start,
        end,
        dag_id,
        run_id,
        settings,
        missing_ids_dataset_id,
        staging_dataset_id,
    ) -> AsyncGenerator[list[SourceT], None]:
        repository = StagingRepository(client)
        if staging_dataset_id is not None:
            source = cls.loader.model.table_name.rsplit(".", 1)[-1]
            dataset = await repository.get(
                staging_dataset_id,
                dag_id=dag_id,
                run_id=run_id,
                timestamp_start=start,
                timestamp_end=end,
                source=source,
            )
            async with aclosing(repository.read_batches(dataset)) as batches:
                async for batch, _ in batches:
                    yield batch
            return

        missing_ids = set()
        if missing_ids_dataset_id is not None:
            dimension = MODEL_DIMENSIONS.get(cls.extractor.model.__tablename__)
            if dimension is None:
                raise ValueError(
                    "Additional dimension IDs require a dimension pipeline"
                )
            dataset = await repository.get(
                missing_ids_dataset_id,
                dag_id=dag_id,
                run_id=run_id,
                timestamp_start=start,
                timestamp_end=end,
                source="collect",
            )
            missing_ids = (await repository.missing_ids(dataset))[dimension]
        async with (
            OltpConnector(settings) as connection,
            connection.session() as session,
        ):
            extractor = cls.extractor(session, settings)
            async with aclosing(
                extractor.read_batches(start, end, missing_ids=missing_ids)
            ) as batches:
                async for batch in batches:
                    yield batch
