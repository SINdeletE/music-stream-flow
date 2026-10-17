import asyncio
from datetime import datetime

import pytest
import src.pipelines.fct_events_pipeline as pipeline_module
from clickhouse_connect.driver.asyncclient import AsyncClient
from confluent_kafka import OFFSET_INVALID, TopicPartition
from src.connections import KafkaConsumerConnector
from src.models.dwh import (
    DimAppRelease,
    DimDevice,
    DimDeviceOs,
    DimEventType,
    DimTrack,
    FctEvent,
)
from src.pipelines.fct_events_pipeline import FctEventsPipeline
from src.settings import DwhSettings

from ._pipeline_data import (
    DAG_ID,
    END,
    NON_UTCOFFSET_IDS,
    NON_UTCOFFSET_INTERVALS,
    PARTITION,
    RECORD_COUNTS,
    RECORD_IDS,
    RUN_ID,
    START,
    assert_audit,
    assert_loaded_rows,
    clear_clickhouse,
    expected_events,
    forbid_connections,
    make_event_payloads,
    prepare_kafka,
    seed_event_dimensions,
)

TASK_ID = "load_events"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("count", RECORD_COUNTS, ids=RECORD_IDS)
async def test_fct_events_pipeline_positive_records(
    client: AsyncClient,
    dwh_settings: DwhSettings,
    kafka_address: str,
    count: int,
):
    """[integration] FctEventsPipeline загружает батчи Kafka и сохраняет следующий offset."""
    await clear_clickhouse(
        client, DimDevice, DimDeviceOs, DimAppRelease, DimTrack, DimEventType, FctEvent
    )
    await seed_event_dimensions(client)
    payloads = make_event_payloads(count)
    kafka_settings = await prepare_kafka(kafka_address, payloads)

    async with asyncio.timeout(120):
        audit = await FctEventsPipeline.run(
            timestamp_start=START,
            timestamp_end=END,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
            partition=PARTITION,
            kafka_settings=kafka_settings,
            dwh_settings=dwh_settings,
        )

    await assert_loaded_rows(
        client,
        FctEvent,
        expected_events(payloads, audit.load_id),
        audit.load_id,
        key="event_id",
    )
    await assert_audit(client, audit, count, task_id=TASK_ID, target=FctEvent)
    async with KafkaConsumerConnector(kafka_settings, PARTITION) as consumer:
        offsets = await asyncio.to_thread(
            consumer.committed,
            [
                TopicPartition(kafka_settings.music_kafka_topic_name, partition)
                for partition in range(kafka_settings.music_kafka_num_partitions)
            ],
            timeout=kafka_settings.music_kafka_timeout,
        )
    assert len(offsets) == 3
    for partition in offsets:
        assert partition.error is None
        expected_offset = (
            count if count and partition.partition == PARTITION else OFFSET_INVALID
        )
        assert partition.offset == expected_offset


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "timestamp_start,timestamp_end", NON_UTCOFFSET_INTERVALS, ids=NON_UTCOFFSET_IDS
)
async def test_fct_events_pipeline_negative_non_utcoffset_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    timestamp_start: datetime,
    timestamp_end: datetime,
):
    """[integration] FctEventsPipeline отклоняет время без timezone до открытия подключений."""
    guards = forbid_connections(monkeypatch, pipeline_module)

    with pytest.raises(ValueError):
        await FctEventsPipeline.run(
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
            partition=PARTITION,
        )

    for guard in guards:
        guard.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_fct_events_pipeline_negative_start_after_end(
    monkeypatch: pytest.MonkeyPatch,
):
    """[integration] FctEventsPipeline отклоняет обратный интервал до открытия подключений."""
    guards = forbid_connections(monkeypatch, pipeline_module)

    with pytest.raises(ValueError):
        await FctEventsPipeline.run(
            timestamp_start=END,
            timestamp_end=START,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
            partition=PARTITION,
        )

    for guard in guards:
        guard.assert_not_called()
