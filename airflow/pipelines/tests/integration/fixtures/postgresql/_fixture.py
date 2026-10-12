import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import asyncpg
from src.settings import OltpSettings
from testcontainers.core.container import DockerContainer

POSTGRESQL_IMAGE = "postgres:18.3-alpine"
DDL_DIRECTORY = Path(__file__).resolve().parents[6] / "sql/music_oltp/ddl"


async def _initialize_database(settings: OltpSettings) -> None:
    async with asyncio.timeout(120):
        while True:
            try:
                connection = await asyncpg.connect(
                    host=settings.music_oltp_db_host,
                    port=settings.music_oltp_db_port,
                    user=settings.music_oltp_db_user,
                    password=settings.music_oltp_db_password.get_secret_value(),
                    database=settings.music_oltp_db_name,
                    timeout=2,
                )
            except (OSError, TimeoutError, asyncpg.PostgresError):
                await asyncio.sleep(0.2)
            else:
                break

        try:
            for path in sorted(DDL_DIRECTORY.glob("*.sql")):
                await connection.execute(path.read_text(encoding="utf-8"))
        finally:
            await connection.close()


@asynccontextmanager
async def postgresql_settings() -> AsyncGenerator[OltpSettings, None]:
    """Yield settings for a temporary PostgreSQL initialized with the OLTP DDL."""
    username = "pipeline_test"
    password = uuid4().hex
    database = "pipeline_test"
    container = (
        DockerContainer(POSTGRESQL_IMAGE)
        .with_kwargs(labels={"music-stream-flow.test-suite": "integration"})
        .with_env("POSTGRES_USER", username)
        .with_env("POSTGRES_PASSWORD", password)
        .with_env("POSTGRES_DB", database)
        .with_exposed_ports(5432)
    )
    try:
        await asyncio.to_thread(container.start)
        settings = OltpSettings(
            _env_file=None,
            music_oltp_db_host=container.get_container_host_ip(),
            music_oltp_db_port=int(container.get_exposed_port(5432)),
            music_oltp_db_user=username,
            music_oltp_db_password=password,
            music_oltp_db_name=database,
            music_oltp_batch_size=4,
        )
        await _initialize_database(settings)
        yield settings
    finally:
        await asyncio.to_thread(container.stop)
