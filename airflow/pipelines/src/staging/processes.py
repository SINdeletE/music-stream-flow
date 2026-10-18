import asyncio
from contextlib import aclosing
from datetime import datetime
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
from confluent_kafka import TopicPartition

from src.connections import DwhConnector, KafkaConsumerConnector, OltpConnector
from src.extractors import KafkaAnalyticsEventExtractor
from src.settings import DwhSettings, KafkaSettings, OltpSettings
from src.transformers._dimension_lookup import lookup_dimension_sks

from .registry import DIMENSIONS, SOURCES
from .repository import Dataset, DatasetRef, StagingRepository, validate_interval


async def find_missing(
    client: AsyncClient, source: str, batch: list
) -> dict[str, set[UUID]]:
    missing = {}
    for name in SOURCES[source].dimensions:
        identifiers = set()
        for entity in batch:
            identifier = getattr(entity, f"{name}_id")
            if identifier is not None:
                identifiers.add(identifier)
        dimension = DIMENSIONS[name]
        found = await lookup_dimension_sks(
            client,
            identifiers,
            table_name=dimension.table,
            entity_name=name,
            version_column=dimension.version,
        )
        missing[name] = {identifier for identifier, sk in found.items() if sk is None}
    return missing


class OltpStaging:
    source: str

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
    ) -> DatasetRef:
        validate_interval(timestamp_start, timestamp_end)
        if oltp_settings is None:
            oltp_settings = OltpSettings()
        async with DwhConnector(dwh_settings) as client:
            repository = StagingRepository(client)
            dataset = Dataset(
                source=cls.source,
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                interval_start=timestamp_start,
                interval_end=timestamp_end,
            )
            async with (
                OltpConnector(oltp_settings) as connection,
                connection.session() as session,
            ):
                extractor = SOURCES[cls.source].extractor(
                    session,
                    oltp_settings,
                    batch_size=oltp_settings.music_oltp_staging_batch_size,
                )
                async with aclosing(
                    extractor.read_batches(timestamp_start, timestamp_end)
                ) as batches:
                    async for batch in batches:
                        missing = await find_missing(client, cls.source, batch)
                        await repository.write_batch(dataset, batch)
                        await repository.write_missing(dataset, missing)
            return await repository.complete(dataset)


class BrgDeviceAppReleaseStaging(OltpStaging):
    source = "brg_device_app_release"


class BrgTrackGenreStaging(OltpStaging):
    source = "brg_track_genre"


class BrgTrackArtistStaging(OltpStaging):
    source = "brg_track_artist"


class BrgTrackReleaseStaging(OltpStaging):
    source = "brg_track_release"


class BrgArtistReleaseStaging(OltpStaging):
    source = "brg_artist_release"


class FctEventsStaging:
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
    ) -> DatasetRef:
        validate_interval(timestamp_start, timestamp_end)
        if kafka_settings is None:
            kafka_settings = KafkaSettings()
        async with DwhConnector(dwh_settings) as client:
            repository = StagingRepository(client)
            dataset = Dataset(
                source="fct_events",
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                interval_start=timestamp_start,
                interval_end=timestamp_end,
                topic=kafka_settings.music_kafka_topic_name,
                partition=partition,
            )
            async with KafkaConsumerConnector(kafka_settings, partition) as consumer:
                committed = await asyncio.to_thread(
                    KafkaConsumerConnector.get_current_offset,
                    consumer,
                    kafka_settings,
                    partition,
                )
                low, high = await asyncio.to_thread(
                    consumer.get_watermark_offsets,
                    consumer.assignment()[0],
                    timeout=kafka_settings.music_kafka_timeout,
                    cached=False,
                )
                dataset.committed_offset = committed
                dataset.first_offset = committed if low <= committed <= high else low
                await asyncio.to_thread(
                    consumer.assign,
                    [TopicPartition(dataset.topic, partition, dataset.first_offset)],
                )
                extractor = KafkaAnalyticsEventExtractor(
                    consumer,
                    kafka_settings,
                    batch_size=kafka_settings.music_kafka_staging_batch_size,
                )
                async with aclosing(
                    extractor.read_batches(timestamp_start, timestamp_end)
                ) as batches:
                    async for batch, next_offset in batches:
                        missing = await find_missing(client, "fct_events", batch)
                        await repository.write_batch(
                            dataset, batch, next_offset=next_offset
                        )
                        await repository.write_missing(dataset, missing)
            return await repository.complete(dataset)


class CollectStaging:
    @classmethod
    async def run(
        cls,
        refs: list[DatasetRef],
        *,
        timestamp_start: datetime,
        timestamp_end: datetime,
        dag_id: str,
        run_id: str,
        task_id: str,
        dwh_settings: DwhSettings | None = None,
    ) -> DatasetRef:
        validate_interval(timestamp_start, timestamp_end)
        async with DwhConnector(dwh_settings) as client:
            repository = StagingRepository(client)
            dataset = Dataset(
                source="collect",
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                interval_start=timestamp_start,
                interval_end=timestamp_end,
            )
            combined = {name: set() for name in DIMENSIONS}
            for identifier in dict.fromkeys(ref["dataset_id"] for ref in refs):
                source = await repository.get(
                    identifier,
                    dag_id=dag_id,
                    run_id=run_id,
                    timestamp_start=timestamp_start,
                    timestamp_end=timestamp_end,
                )
                if source.source == "collect":
                    raise ValueError("Collect requires source staging datasets")
                missing = await repository.missing_ids(source)
                for name, identifiers in missing.items():
                    combined[name].update(identifiers)
            await repository.write_missing(dataset, combined)
            return await repository.complete(dataset)
