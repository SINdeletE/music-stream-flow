from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from src.connections import OltpSettings
from src.models.oltp import Base


EntityT = TypeVar("EntityT", bound=Base)


class OltpExtractor(Generic[EntityT]):
    model: type[EntityT]
    timestamp_column: InstrumentedAttribute[datetime]
    id_columns: tuple[InstrumentedAttribute[UUID], ...]

    def __init__(self, client: AsyncSession, settings: OltpSettings) -> None:
        self.client = client
        self.batch_size = settings.music_oltp_batch_size

    async def read_batches(
        self,
        timestamp_start: datetime,
        timestamp_end: datetime,
    ) -> AsyncGenerator[list[EntityT], None]:
        if timestamp_start.utcoffset() is None or timestamp_end.utcoffset() is None:
            raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
        if timestamp_start > timestamp_end:
            raise ValueError("timestamp_start must not be after timestamp_end")
        if timestamp_start == timestamp_end:
            return

        model = type(self).model
        timestamp_column = type(self).timestamp_column
        id_columns = type(self).id_columns
        statement = (
            select(model)
            .where(
                timestamp_column >= timestamp_start,
                timestamp_column < timestamp_end,
            )
            .order_by(timestamp_column, *id_columns)
            .execution_options(yield_per=self.batch_size)
        )

        result = await self.client.stream_scalars(statement)
        try:
            async for batch in result.partitions(self.batch_size):
                yield list(batch)
        finally:
            await result.close()
