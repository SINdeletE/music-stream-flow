from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.models.dwh import FctEvent
from src.transformers import FctEventsTransformer

from tests.integration.fixtures.clickhouse import clickhouse_client

from ._transformer_data import (
    LOAD_ID,
    clear_dimensions,
    make_event_batch,
    missing_dimension_message,
    seed_dimensions,
)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session() -> AsyncGenerator[AsyncClient, None]:
    async with clickhouse_client() as client:
        yield client


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_fct_events_transformer_positive_records(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] FctEventsTransformer разрешает все SK и сохраняет поля и порядок."""
    await seed_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
        reverse_rows=reverse_rows,
    )
    batch, expected = make_event_batch()
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    actual = await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual is not batch
    assert len(actual) == len(batch)
    assert all(type(row) is FctEvent for row in actual)
    assert actual == expected
    assert all(row.load_id == LOAD_ID for row in actual)
    assert any(row.event_type_sk == 0 for row in actual)
    assert any(row.device_sk == 0 for row in actual)
    assert any(row.device_os_sk == 0 for row in actual)
    assert any(row.app_release_sk == 0 for row in actual)
    assert any(row.track_sk == 0 for row in actual)
    assert actual[-1].track_sk is None
    assert all(row.event_sk is None for row in actual)
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
async def test_fct_events_transformer_positive_empty_batch(session: AsyncClient):
    """[integration] FctEventsTransformer возвращает [] для пустого батча."""
    await clear_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
    )
    batch = []

    actual = await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert actual == []
    assert batch == []


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_fct_events_transformer_negative_missing_event_type_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] FctEventsTransformer отклоняет батч без event_type_sk."""
    await seed_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="event_type",
    )
    batch, _ = make_event_batch()
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("event_type")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_fct_events_transformer_negative_missing_device_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] FctEventsTransformer отклоняет батч без device_sk."""
    await seed_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="device",
    )
    batch, _ = make_event_batch()
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("device")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_fct_events_transformer_negative_missing_device_os_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] FctEventsTransformer отклоняет батч без device_os_sk."""
    await seed_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="device_os",
    )
    batch, _ = make_event_batch()
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("device_os")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_fct_events_transformer_negative_missing_app_release_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] FctEventsTransformer отклоняет батч без app_release_sk."""
    await seed_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="app_release",
    )
    batch, _ = make_event_batch()
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("app_release")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("reverse_rows", [False, True], ids=["forward", "reverse"])
async def test_fct_events_transformer_negative_missing_track_sk(
    session: AsyncClient, reverse_rows: bool
):
    """[integration] FctEventsTransformer отклоняет батч без track_sk."""
    await seed_dimensions(
        session,
        (
            "event_type",
            "device",
            "device_os",
            "app_release",
            "track",
        ),
        reverse_rows=reverse_rows,
        missing_entity_name="track",
    )
    batch, _ = make_event_batch()
    original_order = [id(entity) for entity in batch]
    original_values = [vars(entity).copy() for entity in batch]

    with pytest.raises(LookupError) as error:
        await FctEventsTransformer.transform(batch, session, load_id=LOAD_ID)

    assert str(error.value) == missing_dimension_message("track")
    assert [id(entity) for entity in batch] == original_order
    assert [vars(entity) for entity in batch] == original_values
