from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.mappers.app_release_mapper import AppReleaseMapper
from src.mappers.artist_mapper import ArtistMapper
from src.mappers.device_mapper import DeviceMapper
from src.mappers.device_os_mapper import DeviceOsMapper
from src.mappers.genre_mapper import GenreMapper
from src.mappers.release_mapper import ReleaseMapper
from src.mappers.track_mapper import TrackMapper
from src.models.oltp import (
    AppRelease,
    Artist,
    Device,
    DeviceOsHistory,
    Genre,
    Release,
    Track,
)
from src.transformers import (
    DimAppReleaseTransformer,
    DimArtistTransformer,
    DimDeviceOsTransformer,
    DimDeviceTransformer,
    DimGenreTransformer,
    DimReleaseTransformer,
    DimTrackTransformer,
)

TIMESTAMP = datetime(2026, 10, 11, 12, tzinfo=UTC)
UPDATED = {"updated_at": TIMESTAMP}
DIMENSIONS = (
    (
        "device",
        DimDeviceTransformer,
        DeviceMapper,
        Device,
        "music_dwh.dim_device_all",
        "updated_at",
        {**UPDATED, "name": "Phone", "device_version": "1"},
    ),
    (
        "device_os",
        DimDeviceOsTransformer,
        DeviceOsMapper,
        DeviceOsHistory,
        "music_dwh.dim_device_os_all",
        "valid_from",
        {"os_name": "Android", "os_version": "16", "valid_from": TIMESTAMP},
    ),
    (
        "app_release",
        DimAppReleaseTransformer,
        AppReleaseMapper,
        AppRelease,
        "music_dwh.dim_app_release",
        "source_created_at",
        {
            "platform": "android",
            "app_release_version": "1",
            "build": "1",
            "released_at": TIMESTAMP,
        },
    ),
    (
        "track",
        DimTrackTransformer,
        TrackMapper,
        Track,
        "music_dwh.dim_track_all",
        "updated_at",
        {
            **UPDATED,
            "name": "Track",
            "duration_ms": 180000,
            "is_available": True,
            "deleted_at": None,
        },
    ),
    (
        "artist",
        DimArtistTransformer,
        ArtistMapper,
        Artist,
        "music_dwh.dim_artist_all",
        "updated_at",
        {
            **UPDATED,
            "nickname": "Artist",
            "name": None,
            "middle_name": None,
            "last_name": None,
            "country_code": None,
            "biography": None,
            "is_verified": False,
            "deleted_at": None,
        },
    ),
    (
        "genre",
        DimGenreTransformer,
        GenreMapper,
        Genre,
        "music_dwh.dim_genre",
        "updated_at",
        {**UPDATED, "name": "Genre", "deleted_at": None},
    ),
    (
        "release",
        DimReleaseTransformer,
        ReleaseMapper,
        Release,
        "music_dwh.dim_release_all",
        "updated_at",
        {
            **UPDATED,
            "name": "Album",
            "is_available": True,
            "released_at": TIMESTAMP,
            "deleted_at": None,
        },
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", DIMENSIONS, ids=[case[0] for case in DIMENSIONS])
@pytest.mark.parametrize("scenario", ["empty", "mixed", "new", "query_error"])
async def test_dim_transformer_lookup(case, scenario):
    """[unit] Dim-transformer сохраняет mapping, порядок, SK 0 и новые ID."""
    entity_name, transformer, mapper, model, table_name, version_column, values = case
    client = AsyncMock(spec=AsyncClient)
    load_id = UUID(int=200)
    entities = [
        model(id=UUID(int=index + 1), created_at=TIMESTAMP, **values)
        for index in range(3)
    ]
    batch = [] if scenario == "empty" else [*entities, entities[0]]
    before = [dict(vars(entity)) for entity in batch]
    known = {entities[0].id: 0, entities[1].id: 42} if scenario == "mixed" else {}
    client.query.return_value = SimpleNamespace(result_rows=list(known.items()))

    if scenario == "query_error":
        failure = RuntimeError("Query failed")
        client.query.side_effect = failure
        with pytest.raises(RuntimeError) as error:
            await transformer.transform(batch, client, load_id=load_id)
        assert error.value is failure
    else:
        actual = await transformer.transform(batch, client, load_id=load_id)
        expected = [
            mapper.to_dwh(
                entity, load_id, **{f"{entity_name}_sk": known.get(entity.id)}
            )
            for entity in batch
        ]
        assert actual == expected

    assert [dict(vars(entity)) for entity in batch] == before
    if scenario == "empty":
        client.query.assert_not_called()
    else:
        client.query.assert_awaited_once()
        query_args = client.query.await_args.kwargs
        assert query_args["parameters"] == {
            "entity_ids": [entity.id for entity in entities]
        }
        assert f"FROM {table_name}" in query_args["query"]
        assert (
            f"argMax({entity_name}_sk, ({version_column}, {entity_name}_sk))"
            in query_args["query"]
        )
