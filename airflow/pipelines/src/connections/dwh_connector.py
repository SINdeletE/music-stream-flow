from types import TracebackType

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from src.settings import DwhSettings


class DwhConnector:
    def __init__(self, settings: DwhSettings | None = None) -> None:
        self._settings = settings
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> AsyncClient:
        if self._client is not None:
            raise RuntimeError("DwhConnector is already open")

        settings = self._settings
        if settings is None:
            settings = DwhSettings()  # type: ignore[call-arg]

        self._client = await clickhouse_connect.get_async_client(
            host=settings.music_dwh_db_host,
            port=settings.music_dwh_db_port,
            username=settings.music_dwh_db_user,
            password=settings.music_dwh_db_password.get_secret_value(),
            database=settings.music_dwh_db_name,
        )
        return self._client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            try:
                await self._client.close()
            finally:
                self._client = None
