from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from psycopg import Connection


@dataclass(frozen=True)
class SessionContext:
    installation_id: UUID
    device_id: UUID
    device_os_id: UUID
    app_release_id: UUID


@dataclass(frozen=True)
class SourceSnapshot:
    contexts: tuple[SessionContext, ...] = ()
    track_ids: tuple[UUID, ...] = ()


class OltpSource:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def read_snapshot(self, now: datetime) -> SourceSnapshot:
        if now.utcoffset() is None:
            raise ValueError("Snapshot time must be timezone-aware")
        now = now.astimezone(UTC)
        with self.connection.transaction():
            self.connection.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            contexts = self.connection.execute(
                """
                SELECT
                    i.id AS installation_id, d.id AS device_id,
                    os.id AS device_os_id, a.id AS app_release_id
                FROM music_oltp.installation AS i
                JOIN music_oltp.device AS d ON d.id = i.device_id
                JOIN music_oltp.app_release AS a ON a.id = i.app_release_id
                JOIN LATERAL (
                    SELECT h.id, h.os_name
                    FROM music_oltp.device_os_history AS h
                    WHERE h.device_id = d.id AND h.valid_from <= %(now)s
                        AND h.created_at <= %(now)s
                    ORDER BY h.valid_from DESC, h.id DESC
                    LIMIT 1
                ) AS os ON TRUE
                WHERE i.installed_at <= %(now)s AND i.created_at <= %(now)s
                    AND d.created_at <= %(now)s AND a.created_at <= %(now)s
                    AND a.released_at <= %(now)s
                    AND (
                        (os.os_name = 'iOS' AND a.platform = 'ios')
                        OR (os.os_name = 'Android' AND a.platform = 'android')
                        OR (os.os_name = 'HarmonyOS' AND a.platform = 'harmonyos')
                    )
                ORDER BY i.id
                """,
                {"now": now},
            ).fetchall()
            tracks = self.connection.execute(
                """SELECT id FROM music_oltp.track
                WHERE is_available = TRUE AND deleted_at IS NULL AND created_at <= %s
                ORDER BY id""",
                (now,),
            ).fetchall()
        return SourceSnapshot(
            contexts=tuple(SessionContext(**row) for row in contexts),
            track_ids=tuple(row["id"] for row in tracks),
        )
