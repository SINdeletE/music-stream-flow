import time
from contextlib import closing
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import clickhouse_connect
import pytest
import pytest_asyncio
from testcontainers.core.container import DockerContainer

from ._data import prepare_data

SCHEMA_PATH = Path(__file__).with_name("fixtures") / "schema.sql"


def wait_for_clickhouse(host, port, timeout=120.0):
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
    raise TimeoutError("Temporary ClickHouse did not become ready") from last_error


@pytest.fixture(scope="session", params=[False, True], ids=["forward", "reverse"])
def clickhouse_service(request):
    """Start an isolated ClickHouse for each insertion order without project settings."""
    username = "analytics_test"
    password = uuid4().hex
    container = (
        DockerContainer("clickhouse/clickhouse-server:26.8-alpine")
        .with_kwargs(labels={"music-stream-flow.test-suite": "integration"})
        .with_env("CLICKHOUSE_USER", username)
        .with_env("CLICKHOUSE_PASSWORD", password)
        .with_exposed_ports(8123)
    )
    try:
        container.start()
        parameters = {
            "host": container.get_container_host_ip(),
            "port": int(container.get_exposed_port(8123)),
            "username": username,
            "password": password,
            "database": "default",
        }
        wait_for_clickhouse(parameters["host"], parameters["port"])
        with closing(clickhouse_connect.get_client(**parameters)) as client:
            for statement in SCHEMA_PATH.read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    client.command(statement)
        yield parameters, request.param
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def client(clickhouse_service):
    parameters, reverse_rows = clickhouse_service
    client = await clickhouse_connect.get_async_client(**parameters)
    try:
        await prepare_data(client, reverse_rows=reverse_rows)
        yield client
    finally:
        await client.close()
