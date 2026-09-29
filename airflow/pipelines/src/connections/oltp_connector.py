from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from contextlib import asynccontextmanager
from typing import Self

from .oltp_settings import OltpSettings

oltp_settings = OltpSettings() # type: ignore

class OltpConnector:
    async def __aenter__(self) -> Self:
        self.engine = create_async_engine(url=oltp_settings.asyncpg_database_url)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)

        return self

    @asynccontextmanager
    async def session(self):
        session = self.session_factory()
        try:
            yield session
        finally:
            await session.close()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._close()

    async def _close(self) -> None:
        await self.engine.dispose()
    