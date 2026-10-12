import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import ExitStack, asynccontextmanager, closing
from pathlib import Path
from threading import Lock
from typing import TypedDict
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.settings import DwhSettings
from testcontainers.core.container import DockerContainer

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.8-alpine"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


class _ConnectionParameters(TypedDict):
    host: str
    port: int
    username: str
    password: str
    database: str


def _wait_for_clickhouse(host: str, port: int, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            with closing(
                urlopen(f"http://{host}:{port}/ping", timeout=min(2.0, remaining))
            ) as response:
                if response.status == 200 and response.read().strip() == b"Ok.":
                    return
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
        time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(
        f"ClickHouse HTTP interface was not ready within {timeout}s"
    ) from last_error


class _ClickHouseFixture:
    def __init__(self) -> None:
        self._lock = Lock()
        self._parameters: _ConnectionParameters | None = None
        self._resources: ExitStack | None = None

    def _ensure_started(self) -> _ConnectionParameters:
        with self._lock:
            if self._parameters is not None:
                return self._parameters.copy()

            username = "loader_test"
            password = uuid4().hex
            with ExitStack() as resources:
                container = (
                    DockerContainer(CLICKHOUSE_IMAGE)
                    .with_kwargs(labels={"music-stream-flow.test-suite": "integration"})
                    .with_env("CLICKHOUSE_USER", username)
                    .with_env("CLICKHOUSE_PASSWORD", password)
                    .with_exposed_ports(8123)
                )
                resources.callback(container.stop)
                container.start()
                parameters: _ConnectionParameters = {
                    "host": container.get_container_host_ip(),
                    "port": int(container.get_exposed_port(8123)),
                    "username": username,
                    "password": password,
                    "database": "default",
                }
                _wait_for_clickhouse(parameters["host"], parameters["port"])
                with closing(clickhouse_connect.get_client(**parameters)) as client:
                    for statement in SCHEMA_PATH.read_text(encoding="utf-8").split(";"):
                        if statement.strip():
                            client.command(statement)
                self._resources = resources.pop_all()
                self._parameters = parameters
                return parameters.copy()

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncClient, None]:
        parameters = await asyncio.to_thread(self._ensure_started)
        client = await clickhouse_connect.get_async_client(**parameters)
        try:
            yield client
        finally:
            await client.close()

    def close(self) -> None:
        with self._lock:
            try:
                if self._resources is not None:
                    self._resources.close()
            finally:
                self._resources = None
                self._parameters = None


@asynccontextmanager
async def clickhouse_client() -> AsyncGenerator[AsyncClient, None]:
    """Yield a per-test client connected to the process-wide test container."""
    _fixture = _ClickHouseFixture()
    try:
        async with _fixture.client() as client:
            yield client
    finally:
        _fixture.close()


@asynccontextmanager
async def clickhouse_settings() -> AsyncGenerator[DwhSettings, None]:
    """Yield settings for an isolated container without opening a pipeline client."""
    fixture = _ClickHouseFixture()
    try:
        parameters = await asyncio.to_thread(fixture._ensure_started)
        yield DwhSettings(
            _env_file=None,
            music_dwh_db_host=parameters["host"],
            music_dwh_db_port=parameters["port"],
            music_dwh_db_user=parameters["username"],
            music_dwh_db_password=parameters["password"],
            music_dwh_db_name="music_dwh",
        )
    finally:
        await asyncio.to_thread(fixture.close)
