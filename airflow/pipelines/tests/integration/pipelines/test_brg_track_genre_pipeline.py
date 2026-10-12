import asyncio
from datetime import datetime

import pytest
import src.pipelines._pipeline as pipeline_module
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import BrgTrackGenre, DimGenre, DimTrack
from src.models.oltp import TrackGenre
from src.pipelines import BrgTrackGenrePipeline
from src.settings import DwhSettings, OltpSettings

from ._pipeline_data import (
    DAG_ID,
    END,
    NAIVE_IDS,
    NAIVE_INTERVALS,
    RECORD_COUNTS,
    RECORD_IDS,
    RUN_ID,
    START,
    assert_audit,
    assert_loaded_rows,
    clear_clickhouse,
    forbid_connections,
    make_tracks,
    seed_genres,
    seed_postgresql,
    seed_tracks,
)

TASK_ID = "load_track_genre"


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("count", RECORD_COUNTS, ids=RECORD_IDS)
async def test_brg_track_genre_pipeline_positive_records(
    client: AsyncClient,
    dwh_settings: DwhSettings,
    oltp_settings: OltpSettings,
    count: int,
):
    """[integration] BrgTrackGenrePipeline загружает 0, полные и неполные батчи связей."""
    await clear_clickhouse(client, DimTrack, DimGenre, BrgTrackGenre)
    tracks = make_tracks(count)
    await seed_tracks(client, tracks)
    genres = await seed_genres(client, tracks)
    links = [
        TrackGenre(
            track_id=track.id,
            genre_id=genre.id,
            created_at=track.updated_at,
        )
        for track, genre in zip(tracks, genres, strict=True)
    ]
    await seed_postgresql(oltp_settings, tracks, genres, links)

    async with asyncio.timeout(120):
        audit = await BrgTrackGenrePipeline.run(
            timestamp_start=START,
            timestamp_end=END,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
            oltp_settings=oltp_settings,
            dwh_settings=dwh_settings,
        )

    expected = [
        {
            "track_sk": index * 10,
            "genre_sk": 100 + index * 10,
            "load_id": audit.load_id,
        }
        for index in range(count)
    ]
    await assert_loaded_rows(
        client, BrgTrackGenre, expected, audit.load_id, key="track_sk"
    )
    await assert_audit(client, audit, count, task_id=TASK_ID, target=BrgTrackGenre)


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    "timestamp_start,timestamp_end", NAIVE_INTERVALS, ids=NAIVE_IDS
)
async def test_brg_track_genre_pipeline_negative_naive_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    timestamp_start: datetime,
    timestamp_end: datetime,
):
    """[integration] BrgTrackGenrePipeline отклоняет naive-время до открытия подключений."""
    guards = forbid_connections(monkeypatch, pipeline_module)

    with pytest.raises(ValueError):
        await BrgTrackGenrePipeline.run(
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
        )

    for guard in guards:
        guard.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_brg_track_genre_pipeline_negative_start_after_end(
    monkeypatch: pytest.MonkeyPatch,
):
    """[integration] BrgTrackGenrePipeline отклоняет обратный интервал без подключений."""
    guards = forbid_connections(monkeypatch, pipeline_module)

    with pytest.raises(ValueError):
        await BrgTrackGenrePipeline.run(
            timestamp_start=END,
            timestamp_end=START,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
        )

    for guard in guards:
        guard.assert_not_called()
