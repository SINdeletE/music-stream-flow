from collections.abc import AsyncGenerator

import pytest_asyncio
from clickhouse_connect.driver.asyncclient import AsyncClient
from src.connections import DwhConnector
from src.settings import DwhSettings, OltpSettings

from tests.integration.fixtures.clickhouse import clickhouse_settings
from tests.integration.fixtures.kafka import kafka_bootstrap_servers
from tests.integration.fixtures.postgresql import postgresql_settings


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def dwh_settings() -> AsyncGenerator[DwhSettings, None]:
    async with clickhouse_settings() as settings:
        yield settings


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def client(dwh_settings: DwhSettings) -> AsyncGenerator[AsyncClient, None]:
    async with DwhConnector(dwh_settings) as client:
        yield client


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def oltp_settings() -> AsyncGenerator[OltpSettings, None]:
    async with postgresql_settings() as settings:
        yield settings


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def kafka_address() -> AsyncGenerator[str, None]:
    async with kafka_bootstrap_servers() as address:
        yield address
