import asyncio
import logging
from contextlib import aclosing
from datetime import UTC, datetime
from uuid import UUID, uuid7

from src.connections import DwhConnector, KafkaConsumerConnector
from src.extractors import KafkaAnalyticsEventExtractor
from src.loaders import EtlLoadLoader, FctEventLoader
from src.models.dwh import EtlLoad, FctEvent
from src.settings import DwhSettings, KafkaSettings
from src.staging import Dataset, StagingRepository
from src.transformers import FctEventsTransformer

logger = logging.getLogger(__name__)


class FctEventsPipeline:
    @staticmethod
    def _resume_offset(dataset: Dataset, committed: int) -> int:
        if not dataset.batch_count:
            return committed
        first, last = dataset.first_offset, dataset.next_offset
        if first is None or last is None:
            raise RuntimeError("Staged Kafka offset range is missing")
        if committed < 0:
            return first
        if not first <= committed <= last:
            if committed == dataset.committed_offset:
                return first
            raise RuntimeError("Committed offset is outside the staged range")
        return committed

    @classmethod
    async def run(
        cls,
        *,
        timestamp_start: datetime,
        timestamp_end: datetime,
        dag_id: str,
        run_id: str,
        task_id: str,
        partition: int,
        kafka_settings: KafkaSettings | None = None,
        dwh_settings: DwhSettings | None = None,
        staging_dataset_id: UUID | str | None = None,
    ) -> EtlLoad:
        if timestamp_start.utcoffset() is None or timestamp_end.utcoffset() is None:
            raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
        if timestamp_start > timestamp_end:
            raise ValueError("timestamp_start must not be after timestamp_end")
        if kafka_settings is None:
            kafka_settings = KafkaSettings()  # type: ignore[call-arg]
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
                target_table=FctEvent.table_to_insert(),
                interval_start=timestamp_start,
                interval_end=timestamp_end,
                started_at=datetime.now(UTC),
                state="running",
                rows_read=0,
                rows_written=0,
                rows_rejected=0,
            )

            kafka_offset = -1
            try:
                await audit_loader.load_batch([audit])
                repository = StagingRepository(client)
                dataset = None
                if staging_dataset_id is not None:
                    dataset = await repository.get(
                        staging_dataset_id,
                        dag_id=dag_id,
                        run_id=run_id,
                        timestamp_start=timestamp_start,
                        timestamp_end=timestamp_end,
                        source="fct_events",
                    )
                    if (
                        dataset.topic != kafka_settings.music_kafka_topic_name
                        or dataset.partition != partition
                    ):
                        raise ValueError(
                            "Staging Kafka topic or partition does not match"
                        )
                async with KafkaConsumerConnector(
                    kafka_settings, partition
                ) as consumer:
                    kafka_offset = await asyncio.to_thread(
                        KafkaConsumerConnector.get_current_offset,
                        consumer,
                        kafka_settings,
                        partition,
                    )
                    if dataset is not None:
                        batches = repository.read_batches(dataset)
                        previous_offset = dataset.first_offset
                        kafka_offset = cls._resume_offset(dataset, kafka_offset)
                    else:
                        extractor = KafkaAnalyticsEventExtractor(
                            consumer, kafka_settings
                        )
                        batches = extractor.read_batches(
                            timestamp_start=timestamp_start, timestamp_end=timestamp_end
                        )
                    transformer = FctEventsTransformer()
                    loader = FctEventLoader(client)

                    async with aclosing(batches) as batches:
                        async for source_batch, next_offset in batches:
                            if dataset is not None:
                                if previous_offset < kafka_offset < next_offset:
                                    raise RuntimeError(
                                        "Committed offset is inside a staged batch"
                                    )
                                previous_offset = next_offset
                                if next_offset <= kafka_offset:
                                    continue
                            audit.rows_read += len(source_batch)
                            logger.info(
                                "Extractor read %s records from kafka (offset: %s -> %s) for load_id=%s",
                                len(source_batch),
                                kafka_offset,
                                next_offset,
                                audit.load_id,
                            )

                            kafka_offset = next_offset

                            dwh_batch = await transformer.transform(
                                source_batch, client, load_id=audit.load_id
                            )
                            rows_written = await loader.load_batch(dwh_batch)

                            audit.rows_written += rows_written
                            logger.info(
                                "Loader loaded %s records from kafka (offset: %s -> %s) for load_id=%s",
                                len(source_batch),
                                kafka_offset,
                                next_offset,
                                audit.load_id,
                            )

                            await KafkaConsumerConnector.commit(
                                consumer,
                                kafka_settings,
                                partition,
                                next_offset=kafka_offset,
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
                        "Failed to write audit (offset: %s) for load_id=%s",
                        kafka_offset,
                        audit.load_id,
                    )
                raise

        return audit
