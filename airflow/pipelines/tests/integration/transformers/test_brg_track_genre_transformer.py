from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import BrgTrackGenre
from src.transformers import BrgTrackGenreTransformer

from tests.integration.fixtures.clickhouse import clickhouse_client

from ._transformer_data import (
    LOAD_ID,
    clear_dimensions,
    make_bridge_batch,
    missing_dimension_message,
    seed_dimensions,
)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session() -> AsyncGenerator[AsyncClient, None]:
    async with clickhouse_client() as client:
        yield client


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_brg_track_genre_transformer_positive_records(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] BrgTrackGenreTransformer разрешает все SK и сохраняет поля и порядок."""
    await seed_dimensions(
        session,
        (
            "track",
            "genre",
        ),
        reverse_rows=reverse_rows,
    )
    batch, expected = make_bridge_batch("track_genre")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await BrgTrackGenreTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is BrgTrackGenre for row in actual)
    assert actual == expected
    assert all(row.load_id == LOAD_ID for row in actual)
    assert any(row.track_sk == 0 for row in actual)
    assert any(row.genre_sk == 0 for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
async def test_brg_track_genre_transformer_positive_empty_batch(session: AsyncClient):
    """[integration] BrgTrackGenreTransformer возвращает [] для пустого батча."""
    await clear_dimensions(
        session,
        (
            "track",
            "genre",
        ),
    )
    batch = []

    actual = await BrgTrackGenreTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual == []
    assert batch == []


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_brg_track_genre_transformer_negative_missing_track_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] BrgTrackGenreTransformer отклоняет батч без track_sk."""
    await seed_dimensions(
        session,
        (
            "track",
            "genre",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="track",
    )
    batch, _ = make_bridge_batch("track_genre")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await BrgTrackGenreTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("track")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_brg_track_genre_transformer_negative_missing_genre_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] BrgTrackGenreTransformer отклоняет батч без genre_sk."""
    await seed_dimensions(
        session,
        (
            "track",
            "genre",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="genre",
    )
    batch, _ = make_bridge_batch("track_genre")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await BrgTrackGenreTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("genre")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values
