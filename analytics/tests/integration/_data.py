from datetime import UTC, datetime, timedelta
from uuid import UUID

START = datetime(2026, 10, 18, tzinfo=UTC)
END = datetime(2026, 10, 19, 12, tzinfo=UTC)
LOAD_ID = UUID(int=9000)
USER_A = UUID(int=1000)
USER_B = UUID(int=1001)


async def insert_rows(client, table, rows):
    columns = tuple(rows[0])
    await client.insert(
        table=f"music_dwh.{table}",
        column_names=columns,
        data=[tuple(row[name] for name in columns) for row in rows],
    )


async def prepare_data(client, *, reverse_rows):
    await client.command("SYSTEM STOP MERGES")

    created = {"load_id": LOAD_ID, "source_created_at": START - timedelta(days=20)}
    updated = {**created, "updated_at": START}
    batches = {
        "dim_device_all": [
            {
                **updated,
                "device_sk": index,
                "device_id": UUID(int=10 + index),
                "name": name,
                "device_version": version,
            }
            for index, (name, version) in enumerate(
                (("Phone A", "1.0"), ("Phone B", "1.0"), ("Phone A", "2.0"))
            )
        ],
        "dim_device_os_all": [
            {
                **created,
                "device_os_sk": index,
                "device_os_id": UUID(int=20 + index),
                "name": name,
                "device_os_version": version,
                "valid_from": START - timedelta(days=20),
            }
            for index, (name, version) in enumerate(
                (("Android", "14"), ("iOS", "18"), ("Android", "13"))
            )
        ],
        "dim_track_all": [
            {
                **updated,
                "track_sk": index,
                "track_id": UUID(int=100 + index),
                "name": name,
                "duration_ms": 180000,
                "is_available": index == 0,
                "deleted_at": START if index else None,
            }
            for index, name in enumerate(("Track A", "Track B 🎵"))
        ],
        "dim_artist_all": [
            {
                **updated,
                "artist_sk": index,
                "artist_id": UUID(int=200 + index),
                "nickname": f"Artist {letter}",
                "is_verified": True,
            }
            for index, letter in enumerate("ABC")
        ],
        "dim_release_all": [
            {
                **updated,
                "release_sk": index,
                "release_id": UUID(int=300 + index),
                "name": f"Album {letter}",
                "is_available": True,
                "released_at": START - timedelta(days=20),
            }
            for index, letter in enumerate("AB")
        ],
        "dim_event_type": [
            {"event_type_sk": index, "name": name, "load_id": LOAD_ID}
            for index, name in enumerate(
                (
                    "session_started",
                    "session_ended",
                    "track_selected",
                    "track_liked",
                    "track_disliked",
                )
            )
        ],
        "brg_track_artist_all": [
            {
                "track_sk": track,
                "artist_sk": artist,
                "artist_number": number,
                "load_id": LOAD_ID,
            }
            for track, artist, number in ((0, 0, 1), (0, 1, 2), (1, 1, 1), (1, 2, 2))
        ],
        "brg_track_release_all": [
            {
                "track_sk": track,
                "release_sk": release,
                "track_number": number,
                "disc_number": None,
                "load_id": LOAD_ID,
            }
            for track, release, number in ((0, 0, 1), (0, 1, 1), (1, 0, 2))
        ],
    }
    for table, name_column in (
        ("dim_device_all", "name"),
        ("dim_track_all", "name"),
        ("dim_artist_all", "nickname"),
    ):
        batches[table].append(
            {
                **batches[table][0],
                name_column: "Outdated label",
                "updated_at": START - timedelta(days=1),
            }
        )
    for table in ("brg_track_artist_all", "brg_track_release_all"):
        batches[table] += [{**row, "load_id": UUID(int=9001)} for row in batches[table]]
    for table, rows in batches.items():
        await insert_rows(client, table, list(reversed(rows)) if reverse_rows else rows)

    events = []

    def event(session, kind, timestamp, *, track=None, device=0, os=0, user=USER_A):
        index = len(events)
        events.append(
            {
                "event_sk": index,
                "event_id": UUID(int=2000 + index),
                "event_type_sk": kind,
                "device_sk": device,
                "device_os_sk": os,
                "app_release_sk": 0,
                "user_id": user,
                "track_sk": track,
                "session_id": UUID(int=session),
                "playback_id": None,
                "event_timestamp": timestamp,
                "received_at": timestamp,
                "load_id": LOAD_ID,
            }
        )

    event(1, 0, START)
    event(1, 2, START + timedelta(minutes=1), track=0)
    event(1, 3, START + timedelta(minutes=2), track=0)
    event(1, 2, START + timedelta(minutes=3), track=1)
    event(1, 4, START + timedelta(minutes=4), track=1)
    event(1, 1, START + timedelta(minutes=10, milliseconds=400))
    second = START + timedelta(hours=12)
    event(2, 0, second)
    event(2, 2, second + timedelta(minutes=1), track=0)
    event(2, 3, second + timedelta(minutes=2), track=0)
    event(2, 1, second + timedelta(minutes=20))
    third = END - timedelta(hours=1)
    for kind, minute, track in (
        (0, 0, None),
        (2, 1, 1),
        (3, 2, 1),
        (2, 3, 0),
        (3, 4, 0),
        (2, 6, None),
    ):
        event(
            3,
            kind,
            third + timedelta(minutes=minute),
            track=track,
            device=1,
            os=1,
            user=USER_B,
        )
    event(4, 0, START + timedelta(hours=1), device=1, os=1, user=None)
    event(4, 1, START + timedelta(hours=1, minutes=5), device=1, os=1, user=None)
    event(5, 1, START + timedelta(hours=2), device=1, os=1)
    event(6, 1, START + timedelta(hours=4), device=1, os=1)
    event(6, 0, START + timedelta(hours=5), device=1, os=1)
    old = START - timedelta(days=10)
    for kind, minute, track in ((0, 0, None), (2, 1, 0), (3, 2, 0), (1, 15, None)):
        event(7, kind, old + timedelta(minutes=minute), track=track)
    event(8, 2, END - timedelta(days=7), track=1, device=1, os=1)
    event(9, 0, START - timedelta(days=2), device=2, os=2)
    event(9, 1, START - timedelta(days=2) + timedelta(minutes=5), device=2, os=2)
    event(10, 2, END, track=1)
    event(10, 3, END, track=1)
    event(3, 1, END + timedelta(minutes=30), device=1, os=1, user=USER_B)
    await insert_rows(
        client, "fct_events_all", list(reversed(events)) if reverse_rows else events
    )
    duplicates = [
        {**row, "event_sk": 10000 + index, "load_id": UUID(int=9002)}
        for index, row in enumerate(events[:6])
    ]
    await insert_rows(client, "fct_events_all", duplicates)
