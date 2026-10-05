from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .oltp_settings import OltpSettings


class OltpConnector:
    def __init__(self, settings: OltpSettings | None = None) -> None:
        self._settings = settings
        self.engine: AsyncEngine | None = None
        self.session_factory: async_sessionmaker[AsyncSession] | None = None

    async def __aenter__(self) -> Self:
        if self.engine is not None:
            raise RuntimeError("OltpConnector is already open")

        settings = self._settings
        if settings is None:
            settings = OltpSettings()  # type: ignore[call-arg]

        self.engine = create_async_engine(url=settings.database_url)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        return self

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self.session_factory is None:
            raise RuntimeError("OltpConnector is not open")

        session = self.session_factory()
        try:
            yield session
        finally:
            await session.close()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._close()

    async def _close(self) -> None:
        if self.engine is not None:
            try:
                await self.engine.dispose()
            finally:
                self.engine = None
                self.session_factory = None
