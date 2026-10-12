from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.transformers import lookup_dimension_sks, resolve_dimension_sks


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup", [lookup_dimension_sks, resolve_dimension_sks])
async def test_empty_lookup(lookup):
    """[unit] Пустой вход возвращает словарь без запроса."""
    client = AsyncMock(spec=AsyncClient)

    actual = await lookup(
        client, [], table_name="music_dwh.dim_device_all", entity_name="device"
    )

    assert actual == {}
    client.query.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version_column", ["updated_at", "valid_from", "source_created_at"]
)
async def test_lookup_unique_ids_and_missing_keys(version_column):
    """[unit] Поиск принимает генератор ID, сохраняет SK 0 и None для новых ID."""
    client = AsyncMock(spec=AsyncClient)
    found_id, missing_id = UUID(int=1), UUID(int=2)
    client.query.return_value = SimpleNamespace(result_rows=[(found_id, 0)])
    entity_ids = (entity_id for entity_id in [found_id, missing_id, found_id])

    actual = await lookup_dimension_sks(
        client,
        entity_ids,
        table_name="music_dwh.dim_device_all",
        entity_name="device",
        version_column=version_column,
    )

    assert actual == {found_id: 0, missing_id: None}
    client.query.assert_awaited_once()
    query_args = client.query.await_args.kwargs
    assert query_args["parameters"] == {"entity_ids": [found_id, missing_id]}
    assert "FROM music_dwh.dim_device_all" in query_args["query"]
    assert f"argMax(device_sk, ({version_column}, device_sk))" in query_args["query"]


@pytest.mark.asyncio
async def test_resolve_found_ids():
    """[unit] Строгий lookup возвращает найденные SK, включая 0."""
    client = AsyncMock(spec=AsyncClient)
    first_id, second_id = UUID(int=1), UUID(int=2)
    client.query.return_value = SimpleNamespace(
        result_rows=[(second_id, 42), (first_id, 0)]
    )

    actual = await resolve_dimension_sks(
        client,
        [first_id, second_id],
        table_name="music_dwh.dim_device_all",
        entity_name="device",
    )

    assert actual == {first_id: 0, second_id: 42}
    client.query.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_missing_id():
    """[unit] Строгий lookup сообщает таблицу и ID отсутствующего измерения."""
    client = AsyncMock(spec=AsyncClient)
    found_id, missing_id = UUID(int=1), UUID(int=2)
    client.query.return_value = SimpleNamespace(result_rows=[(found_id, 0)])

    with pytest.raises(LookupError) as error:
        await resolve_dimension_sks(
            client,
            [found_id, missing_id],
            table_name="music_dwh.dim_device_all",
            entity_name="device",
        )

    assert str(error.value) == (
        f"Dimension not found in music_dwh.dim_device_all: device_id={missing_id}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup", [lookup_dimension_sks, resolve_dimension_sks])
async def test_lookup_query_error(lookup):
    """[unit] Ошибка запроса передаётся вызывающему коду без замены."""
    client = AsyncMock(spec=AsyncClient)
    failure = RuntimeError("Query failed")
    client.query.side_effect = failure

    with pytest.raises(RuntimeError) as error:
        await lookup(
            client,
            [UUID(int=1)],
            table_name="music_dwh.dim_device_all",
            entity_name="device",
        )

    assert error.value is failure
