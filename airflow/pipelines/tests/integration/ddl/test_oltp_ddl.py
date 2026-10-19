import asyncpg
import pytest

from tests.integration.fixtures.postgresql import postgresql_settings
from tests.integration.fixtures.postgresql._fixture import DDL_DIRECTORY

TABLES = {
    "app_release",
    "artist",
    "artist_release",
    "device",
    "device_os_history",
    "genre",
    "installation",
    "release",
    "track",
    "track_artist",
    "track_genre",
    "track_release",
}


@pytest.mark.asyncio
async def test_oltp_ddl_positive_drop_all_and_recreate():
    """[integration] Manual reset drops only OLTP tables and permits repeated initialization."""
    async with postgresql_settings() as settings:
        connection = await asyncpg.connect(
            host=settings.music_oltp_db_host,
            port=settings.music_oltp_db_port,
            user=settings.music_oltp_db_user,
            password=settings.music_oltp_db_password.get_secret_value(),
            database=settings.music_oltp_db_name,
        )
        try:
            query = "SELECT tablename FROM pg_tables WHERE schemaname = 'music_oltp'"
            assert {row["tablename"] for row in await connection.fetch(query)} == TABLES
            await connection.execute("CREATE TABLE public.reset_sentinel (id integer)")
            script = (DDL_DIRECTORY / "drop-all.sql").read_text(encoding="utf-8")

            await connection.execute(script)
            assert await connection.fetch(query) == []
            assert (
                await connection.fetchval("SELECT to_regnamespace('music_oltp')")
                is not None
            )
            assert (
                await connection.fetchval("SELECT to_regclass('public.reset_sentinel')")
                is not None
            )
            await connection.execute(script)

            for path in sorted(DDL_DIRECTORY.glob("[0-9]*.sql")):
                await connection.execute(path.read_text(encoding="utf-8"))
            assert {row["tablename"] for row in await connection.fetch(query)} == TABLES
        finally:
            await connection.close()
