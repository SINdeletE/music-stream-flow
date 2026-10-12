from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import BrgArtistRelease
from src.transformers import BrgArtistReleaseTransformer

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
async def test_brg_artist_release_transformer_positive_records(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] BrgArtistReleaseTransformer разрешает все SK и сохраняет поля и порядок."""
    await seed_dimensions(
        session,
        (
            "artist",
            "release",
        ),
        reverse_rows=reverse_rows,
    )
    batch, expected = make_bridge_batch("artist_release")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await BrgArtistReleaseTransformer.transform(
        batch, session, load_id=LOAD_ID
    )

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is BrgArtistRelease for row in actual)
    assert actual == expected
    assert all(row.load_id == LOAD_ID for row in actual)
    assert any(row.artist_sk == 0 for row in actual)
    assert any(row.release_sk == 0 for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
async def test_brg_artist_release_transformer_positive_empty_batch(
    session: AsyncClient,
):
    """[integration] BrgArtistReleaseTransformer возвращает [] для пустого батча."""
    await clear_dimensions(
        session,
        (
            "artist",
            "release",
        ),
    )
    batch = []

    actual = await BrgArtistReleaseTransformer.transform(
        batch, session, load_id=LOAD_ID
    )

    assert actual == []
    assert batch == []


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_brg_artist_release_transformer_negative_missing_artist_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] BrgArtistReleaseTransformer отклоняет батч без artist_sk."""
    await seed_dimensions(
        session,
        (
            "artist",
            "release",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="artist",
    )
    batch, _ = make_bridge_batch("artist_release")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await BrgArtistReleaseTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("artist")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_brg_artist_release_transformer_negative_missing_release_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] BrgArtistReleaseTransformer отклоняет батч без release_sk."""
    await seed_dimensions(
        session,
        (
            "artist",
            "release",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="release",
    )
    batch, _ = make_bridge_batch("artist_release")
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await BrgArtistReleaseTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("release")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values
