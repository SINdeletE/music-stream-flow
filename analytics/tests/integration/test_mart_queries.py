import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ._data import END, START, USER_A, USER_B

QUERIES = Path(__file__).resolve().parents[2] / "queries"
PARAMETERS = {
    "timestamp_start": START,
    "timestamp_end": END,
    "as_of": END,
    "limit": 10,
}
SCRIPT_PARAMETERS = {
    "timestamp_start": datetime(2026, 10, 1, tzinfo=UTC),
    "timestamp_end": datetime(2026, 10, 20, tzinfo=UTC),
    "as_of": datetime(2026, 10, 20, tzinfo=UTC),
    "limit": 10,
}
EXPECTED = {
    "top_active_device_models": [
        {
            "device_name": "Phone B",
            "events_count": 11,
            "sessions_count": 4,
            "last_seen_at": END - timedelta(minutes=54),
        },
        {
            "device_name": "Phone A",
            "events_count": 12,
            "sessions_count": 3,
            "last_seen_at": START + timedelta(hours=12, minutes=20),
        },
    ],
    "top_active_operating_systems": [
        {
            "os_name": "iOS",
            "events_count": 11,
            "sessions_count": 4,
            "last_seen_at": END - timedelta(minutes=54),
        },
        {
            "os_name": "Android",
            "events_count": 12,
            "sessions_count": 3,
            "last_seen_at": START + timedelta(hours=12, minutes=20),
        },
    ],
    "top_tracks_by_selections": [
        {"track_id": UUID(int=100), "track_name": "Track A", "selections_count": 3},
        {"track_id": UUID(int=101), "track_name": "Track B 🎵", "selections_count": 2},
    ],
    "top_tracks_by_likes": [
        {"track_id": UUID(int=100), "track_name": "Track A", "likes_count": 3},
        {"track_id": UUID(int=101), "track_name": "Track B 🎵", "likes_count": 1},
    ],
    "top_tracks_by_likes_by_period": [
        {"track_id": UUID(int=100), "track_name": "Track A", "likes_count": 3},
        {"track_id": UUID(int=101), "track_name": "Track B 🎵", "likes_count": 1},
    ],
    "top_releases_by_selections": [
        {"release_id": UUID(int=300), "release_name": "Album A", "selections_count": 5},
        {"release_id": UUID(int=301), "release_name": "Album B", "selections_count": 3},
    ],
    "top_artists_by_selections": [
        {
            "artist_id": UUID(int=201),
            "artist_nickname": "Artist B",
            "selections_count": 5,
        },
        {
            "artist_id": UUID(int=200),
            "artist_nickname": "Artist A",
            "selections_count": 3,
        },
        {
            "artist_id": UUID(int=202),
            "artist_nickname": "Artist C",
            "selections_count": 2,
        },
    ],
    "average_session_duration": [
        {
            "sessions_count": 4,
            "completed_sessions_count": 3,
            "open_sessions_count": 1,
            "average_duration_seconds": 1425.1,
        },
    ],
    "average_session_duration_by_user": [
        {
            "user_id": USER_B,
            "sessions_count": 1,
            "completed_sessions_count": 0,
            "open_sessions_count": 1,
            "average_duration_seconds": 3600.0,
        },
        {
            "user_id": USER_A,
            "sessions_count": 2,
            "completed_sessions_count": 2,
            "open_sessions_count": 0,
            "average_duration_seconds": 900.2,
        },
    ],
}


def read_script(name):
    script = (QUERIES / f"{name}.sql").read_text(encoding="utf-8")
    return [statement.strip() for statement in script.split(";") if statement.strip()]


async def run_script(client, name):
    statements = read_script(name)
    settings = {"session_id": uuid4().hex}
    for statement in statements[:-1]:
        await client.command(statement, settings=settings)
    result = await client.query(
        statements[-1],
        settings=settings,
        tz_mode="aware",
    )
    return list(result.named_results())


async def run_query(client, name, **overrides):
    result = await client.query(
        read_script(name)[-1],
        parameters={**PARAMETERS, **overrides},
        tz_mode="aware",
    )
    return list(result.named_results())


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("name", EXPECTED)
async def test_mart_queries_positive_initialization(client, name):
    """[integration] Every script initializes all parameters and runs without external values."""
    script = (QUERIES / f"{name}.sql").read_text(encoding="utf-8")
    initialized = re.findall(r"SET\s+param_(\w+)\s*=", script)
    required = set(re.findall(r"\{(\w+):", script))
    assert len(initialized) == len(required)
    assert set(initialized) == required

    actual = await run_script(client, name)
    parameters = {
        **SCRIPT_PARAMETERS,
        "timestamp_start": datetime(1970, 1, 1, tzinfo=UTC)
        if name == "top_tracks_by_likes"
        else SCRIPT_PARAMETERS["timestamp_start"],
    }
    expected = await run_query(client, name, **parameters)
    assert actual == expected


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("name", EXPECTED)
async def test_mart_queries_positive_records(client, name):
    """[integration] Mart queries preserve counts despite fact, bridge and dimension duplicates."""
    assert await run_query(client, name) == EXPECTED[name]


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("name", EXPECTED)
async def test_mart_queries_positive_empty_interval(client, name):
    """[integration] Empty windows yield empty rankings or a null average with zero sessions."""
    actual = await run_query(
        client,
        name,
        timestamp_start=END + timedelta(days=1),
        timestamp_end=END + timedelta(days=2),
        as_of=START - timedelta(days=30),
    )
    assert actual == (
        [
            {
                "sessions_count": 0,
                "completed_sessions_count": 0,
                "open_sessions_count": 0,
                "average_duration_seconds": None,
            }
        ]
        if name == "average_session_duration"
        else []
    )


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("name", [name for name in EXPECTED if name.startswith("top_")])
async def test_mart_queries_positive_limit(client, name):
    """[integration] Top limits apply to the complete selected reporting interval."""
    expected = EXPECTED[name][:1]
    assert await run_query(client, name, limit=1) == expected


@pytest.mark.asyncio(loop_scope="session")
async def test_mart_queries_positive_all_time_likes(client):
    """[integration] All-time likes include events preceding the selected reporting window."""
    actual = await run_query(
        client, "top_tracks_by_likes", timestamp_start=datetime(1970, 1, 1, tzinfo=UTC)
    )
    assert actual == [
        {"track_id": UUID(int=100), "track_name": "Track A", "likes_count": 4},
        {"track_id": UUID(int=101), "track_name": "Track B 🎵", "likes_count": 1},
    ]


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    ("timestamp_start", "timestamp_end", "expected"),
    [
        (
            START,
            START + timedelta(days=1),
            [{"track_id": UUID(int=100), "track_name": "Track A", "likes_count": 2}],
        ),
        (
            START + timedelta(days=1),
            END,
            [
                {"track_id": UUID(int=100), "track_name": "Track A", "likes_count": 1},
                {
                    "track_id": UUID(int=101),
                    "track_name": "Track B 🎵",
                    "likes_count": 1,
                },
            ],
        ),
    ],
    ids=["first_day", "second_day"],
)
async def test_mart_queries_positive_likes_in_selected_interval(
    client, timestamp_start, timestamp_end, expected
):
    """[integration] Likes form one top over the requested interval without calendar buckets."""
    actual = await run_query(
        client,
        "top_tracks_by_likes_by_period",
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
    )
    assert actual == expected
