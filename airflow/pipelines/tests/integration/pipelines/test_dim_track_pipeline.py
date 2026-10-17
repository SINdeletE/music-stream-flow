import asyncio
from datetime import datetime

import pytest
import src.pipelines._pipeline as pipeline_module
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import DimTrack
from src.pipelines import DimTrackPipeline
from src.settings import DwhSettings, OltpSettings

from ._pipeline_data import (
    DAG_ID,
    END,
    NON_UTCOFFSET_IDS,
    NON_UTCOFFSET_INTERVALS,
    RECORD_COUNTS,
    RECORD_IDS,
    RUN_ID,
    START,
    assert_audit,
    assert_loaded_rows,
    clear_clickhouse,
    expected_tracks,
    forbid_connections,
    make_tracks,
    seed_postgresql,
    seed_tracks,
)

TASK_ID = "load_track"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("count", RECORD_COUNTS, ids=RECORD_IDS)
async def test_dim_track_pipeline_positive_records(
    client: AsyncClient,
    dwh_settings: DwhSettings,
    oltp_settings: OltpSettings,
    count: int,
):
    """[integration] DimTrackPipeline загружает 0, полные и неполные батчи из PostgreSQL."""
    await clear_clickhouse(client, DimTrack)
    tracks = make_tracks(count)
    await seed_postgresql(oltp_settings, tracks)
    await seed_tracks(client, tracks)

    async with asyncio.timeout(120):
        audit = await DimTrackPipeline.run(
            timestamp_start=START,
            timestamp_end=END,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
            oltp_settings=oltp_settings,
            dwh_settings=dwh_settings,
        )

    await assert_loaded_rows(
        client,
        DimTrack,
        expected_tracks(tracks, count, audit.load_id),
        audit.load_id,
        key="track_id",
    )
    await assert_audit(client, audit, count, task_id=TASK_ID, target=DimTrack)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "timestamp_start,timestamp_end", NON_UTCOFFSET_INTERVALS, ids=NON_UTCOFFSET_IDS
)
async def test_dim_track_pipeline_negative_non_utcoffset_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    timestamp_start: datetime,
    timestamp_end: datetime,
):
    """[integration] DimTrackPipeline отклоняет время без timezone до открытия подключений."""
    guards = forbid_connections(monkeypatch, pipeline_module)

    with pytest.raises(ValueError):
        await DimTrackPipeline.run(
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
        )

    for guard in guards:
        guard.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_dim_track_pipeline_negative_start_after_end(
    monkeypatch: pytest.MonkeyPatch,
):
    """[integration] DimTrackPipeline отклоняет обратный интервал до открытия подключений."""
    guards = forbid_connections(monkeypatch, pipeline_module)

    with pytest.raises(ValueError):
        await DimTrackPipeline.run(
            timestamp_start=END,
            timestamp_end=START,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
        )

    for guard in guards:
        guard.assert_not_called()
