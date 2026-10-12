from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from src.models.oltp import Track


@asynccontextmanager
async def sqlite_session() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        execution_options={"schema_translate_map": {"music_oltp": None}},
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Track.__table__.create)

        async with AsyncSession(engine, expire_on_commit=False) as session:
            yield session
    finally:
        await engine.dispose()
