from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Any
from uuid import UUID

from psycopg import Connection, sql

TABLES = frozenset(
    {
        "device",
        "device_os_history",
        "genre",
        "artist",
        "app_release",
        "release",
        "installation",
        "track",
        "track_genre",
        "track_artist",
        "track_release",
        "artist_release",
    }
)


class Repository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def transaction(self) -> AbstractContextManager:
        return self.connection.transaction()

    @staticmethod
    def table(name: str) -> sql.Identifier:
        if name not in TABLES:
            raise ValueError("Unsupported OLTP table")
        return sql.Identifier("music_oltp", name)

    def validate_schema(self) -> None:
        for table in TABLES:
            self.connection.execute(
                sql.SQL("SELECT 1 FROM {} LIMIT 0").format(self.table(table))
            )

    def count(self, table: str) -> int:
        row = self.connection.execute(
            sql.SQL("SELECT count(*) AS count FROM {}").format(self.table(table))
        ).fetchone()
        return row["count"]

    def rows(self, table: str, **filters: Any) -> list[dict]:
        clauses = [sql.SQL("{} = %s").format(sql.Identifier(key)) for key in filters]
        query = sql.SQL("SELECT * FROM {}").format(self.table(table))
        if clauses:
            query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)
        return self.connection.execute(query, tuple(filters.values())).fetchall()

    def sample(
        self, table: str, limit: int = 1, *, excluded: Sequence[UUID] = ()
    ) -> list[dict]:
        query = sql.SQL(
            "SELECT * FROM {} WHERE NOT (id = ANY(%s::uuid[])) ORDER BY random() LIMIT %s"
        ).format(self.table(table))
        return self.connection.execute(query, (list(excluded), limit)).fetchall()

    def insert(self, table: str, row: Mapping[str, Any]) -> None:
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            self.table(table),
            sql.SQL(", ").join(map(sql.Identifier, row)),
            sql.SQL(", ").join(sql.Placeholder() for _ in row),
        )
        self.connection.execute(query, tuple(row.values()))

    def update(self, table: str, entity_id: UUID, values: Mapping[str, Any]) -> None:
        query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(
            self.table(table),
            sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(key)) for key in values
            ),
        )
        self.connection.execute(query, (*values.values(), entity_id))

    def next_track_number(self, release_id: UUID) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(track_number), 0) + 1 AS number FROM music_oltp.track_release WHERE release_id = %s",
            (release_id,),
        ).fetchone()
        return row["number"]
