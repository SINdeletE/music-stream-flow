from datetime import UTC, datetime, timedelta
from uuid import uuid4

from psycopg import sql


def insert(connection, table, **row):
    connection.execute(
        sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier("music_oltp", table),
            sql.SQL(", ").join(map(sql.Identifier, row)),
            sql.SQL(", ").join(sql.Placeholder() for _ in row),
        ),
        tuple(row.values()),
    )


def populate(connection, count=3):
    now = datetime.now(UTC) - timedelta(seconds=10)
    artist_id, release_id, genre_id = uuid4(), uuid4(), uuid4()
    insert(
        connection,
        "artist",
        id=artist_id,
        nickname="Test Artist",
        created_at=now,
        updated_at=now,
    )
    insert(
        connection,
        "release",
        id=release_id,
        name="Test Album",
        is_available=True,
        released_at=now,
        created_at=now,
        updated_at=now,
    )
    insert(
        connection, "genre", id=genre_id, name="Rock", created_at=now, updated_at=now
    )
    insert(
        connection,
        "artist_release",
        artist_id=artist_id,
        release_id=release_id,
        artist_number=1,
        created_at=now,
        updated_at=now,
    )
    contexts, tracks = [], []
    for index in range(count):
        device_id, os_id, app_id, installation_id, track_id = (
            uuid4() for _ in range(5)
        )
        insert(
            connection,
            "device",
            id=device_id,
            name=f"Test Phone {index}",
            device_version="1.0",
            created_at=now,
            updated_at=now,
        )
        insert(
            connection,
            "device_os_history",
            id=os_id,
            device_id=device_id,
            os_name="Android",
            os_version="14",
            valid_from=now,
            created_at=now,
        )
        insert(
            connection,
            "app_release",
            id=app_id,
            platform="android",
            app_release_version="1.0",
            build=str(index),
            released_at=now,
            created_at=now,
        )
        insert(
            connection,
            "installation",
            id=installation_id,
            device_id=device_id,
            app_release_id=app_id,
            installed_at=now,
            created_at=now,
            updated_at=now,
        )
        insert(
            connection,
            "track",
            id=track_id,
            name=f"Test Track {index}",
            duration_ms=180000,
            is_available=True,
            created_at=now,
            updated_at=now,
        )
        insert(
            connection,
            "track_artist",
            track_id=track_id,
            artist_id=artist_id,
            artist_number=1,
            created_at=now,
            updated_at=now,
        )
        insert(
            connection,
            "track_genre",
            track_id=track_id,
            genre_id=genre_id,
            created_at=now,
        )
        insert(
            connection,
            "track_release",
            track_id=track_id,
            release_id=release_id,
            track_number=index + 1,
            created_at=now,
            updated_at=now,
        )
        contexts.append(
            {
                "installation_id": installation_id,
                "device_id": device_id,
                "device_os_id": os_id,
                "app_release_id": app_id,
            }
        )
        tracks.append(track_id)
    return contexts, tracks


def database_state(connection):
    tables = (
        "device",
        "device_os_history",
        "app_release",
        "installation",
        "artist",
        "genre",
        "release",
        "track",
        "track_genre",
        "track_artist",
        "track_release",
        "artist_release",
    )
    return {
        table: sorted(
            (
                dict(row)
                for row in connection.execute(
                    sql.SQL("SELECT * FROM {}").format(
                        sql.Identifier("music_oltp", table)
                    )
                ).fetchall()
            ),
            key=lambda row: str(sorted(row.items())),
        )
        for table in tables
    }
