from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import DimAppRelease
from src.transformers import DimAppReleaseTransformer

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
async def test_dim_app_release_transformer_positive_records(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] DimAppReleaseTransformer сохраняет поля, порядок и последние SK."""
    await seed_dimensions(session, ("app_release",), reverse_rows=reverse_rows)
    batch, expected = make_dimension_batch("app_release")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await DimAppReleaseTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is DimAppRelease for row in actual)
    assert actual == expected
    assert all(row.load_id == LOAD_ID for row in actual)
    assert any(row.app_release_sk == 0 for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
async def test_dim_app_release_transformer_positive_empty_batch(session: AsyncClient):
    """[integration] DimAppReleaseTransformer возвращает [] для пустого батча."""
    await clear_dimensions(session, ("app_release",))
    batch = []

    actual = await DimAppReleaseTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual == []
    assert batch == []


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_dim_app_release_transformer_negative_missing_app_release_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] DimAppReleaseTransformer оставляет None только для отсутствующего ID."""
    await seed_dimensions(
        session,
        ("app_release",),
        reverse_rows=reverse_rows,
        missing_entity_name="app_release",
    )
    batch, expected = make_dimension_batch("app_release", missing_sk=True)
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await DimAppReleaseTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is DimAppRelease for row in actual)
    assert actual == expected
    assert [row.app_release_sk for row in actual] == [None, 0, None]
    assert all(row.load_id == LOAD_ID for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values
