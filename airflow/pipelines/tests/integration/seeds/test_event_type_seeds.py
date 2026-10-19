from pathlib import Path
from uuid import UUID

import pytest
from src.models.kafka.analytics_event import EventType

from tests.integration.fixtures.clickhouse import clickhouse_client

SEED_PATH = Path(__file__).resolve().parents[5] / "sql/dwh/ddl/04-seeds.sql"


@pytest.mark.asyncio
async def test_event_type_seeds_positive_records():
    """[integration] Seeds add all event types without changing existing keys on reruns."""
    script = SEED_PATH.read_text(encoding="utf-8")
    bootstrap_load_id = UUID(int=0)
    existing_load_id = UUID(int=123)
    async with clickhouse_client() as client:
        await client.insert(
            "music_dwh.dim_event_type",
            [(42, "session_started", existing_load_id, None)],
            column_names=("event_type_sk", "name", "load_id", "deleted_at"),
        )
        await client.command(script)
        first = await client.query(
            "SELECT * FROM music_dwh.dim_event_type ORDER BY name"
        )
        rows = list(first.named_results())
        assert len(rows) == len(EventType)
        assert {row["name"] for row in rows} == {event.value for event in EventType}
        existing = next(row for row in rows if row["name"] == "session_started")
        assert existing["event_type_sk"] == 42
        assert existing["load_id"] == existing_load_id
        assert all(row["deleted_at"] is None for row in rows)
        assert all(
            row["load_id"] == bootstrap_load_id
            for row in rows
            if row["name"] != "session_started"
        )

        await client.command(script)
        repeated = await client.query(
            "SELECT * FROM music_dwh.dim_event_type ORDER BY name"
        )
        assert list(repeated.named_results()) == rows
