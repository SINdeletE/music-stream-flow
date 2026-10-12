from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import DimDevice
from src.transformers import DimDeviceTransformer

from tests.integration.fixtures.clickhouse import clickhouse_client

from ._transformer_data import (
    LOAD_ID,
    clear_dimensions,
    make_dimension_batch,
    seed_dimensions,
)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session() -> AsyncGenerator[AsyncClient, None]:
    async with clickhouse_client() as client:
        yield client


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_dim_device_transformer_positive_records(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] DimDeviceTransformer сохраняет поля, порядок и последние SK."""
    await seed_dimensions(session, ("device",), reverse_rows=reverse_rows)
    batch, expected = make_dimension_batch("device")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await DimDeviceTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is DimDevice for row in actual)
    assert actual == expected
    assert all(row.load_id == LOAD_ID for row in actual)
    assert any(row.device_sk == 0 for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
async def test_dim_device_transformer_positive_empty_batch(session: AsyncClient):
    """[integration] DimDeviceTransformer возвращает [] для пустого батча."""
    await clear_dimensions(session, ("device",))
    batch = []

    actual = await DimDeviceTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual == []
    assert batch == []


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_dim_device_transformer_negative_missing_device_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] DimDeviceTransformer оставляет None только для отсутствующего ID."""
    await seed_dimensions(
        session,
        ("device",),
        reverse_rows=reverse_rows,
        missing_entity_name="device",
    )
    batch, expected = make_dimension_batch("device", missing_sk=True)
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await DimDeviceTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is DimDevice for row in actual)
    assert actual == expected
    assert [row.device_sk for row in actual] == [None, 0, None]
    assert all(row.load_id == LOAD_ID for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values
