from collections.abc import AsyncGenerator, Iterable
from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import and_, any_, bindparam, or_, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from src.models.oltp import Base
from src.settings import OltpSettings

EntityT = TypeVar("EntityT", bound=Base)


class OltpExtractor(Generic[EntityT]):
    model: type[EntityT]
    timestamp_column: InstrumentedAttribute[datetime]
    id_columns: tuple[InstrumentedAttribute[UUID], ...]

    def __init__(
        self,
        client: AsyncSession,
        settings: OltpSettings,
        *,
        batch_size: int | None = None,
    ) -> None:
        self.client = client
        self.batch_size = (
            settings.music_oltp_batch_size if batch_size is None else batch_size
        )
        if self.batch_size < 1:
            raise ValueError("batch_size must be positive")

    async def read_batches(
        self,
        timestamp_start: datetime,
        timestamp_end: datetime,
        *,
        missing_ids: Iterable[UUID] = (),
    ) -> AsyncGenerator[list[EntityT], None]:
        if timestamp_start.utcoffset() is None or timestamp_end.utcoffset() is None:
            raise ValueError("timestamp_start and timestamp_end must be timezone-aware")
        if timestamp_start > timestamp_end:
            raise ValueError("timestamp_start must not be after timestamp_end")
        extra_ids = set(missing_ids)
        if timestamp_start == timestamp_end and not extra_ids:
            return

        model = type(self).model
        timestamp_column = type(self).timestamp_column
        id_columns = type(self).id_columns
        predicate = and_(
            timestamp_column >= timestamp_start, timestamp_column < timestamp_end
        )
        if extra_ids:
            if len(id_columns) != 1:
                raise ValueError("missing_ids requires a single-column identifier")
            identifier = id_columns[0]
            if self.client.get_bind().dialect.name == "postgresql":
                ids = bindparam(
                    "missing_ids",
                    value=list(extra_ids),
                    type_=ARRAY(PgUUID(as_uuid=True)),
                )
                predicate = or_(predicate, identifier == any_(ids))
            else:
                predicate = or_(predicate, identifier.in_(extra_ids))
        statement = (
            select(model)
            .where(predicate)
            .order_by(timestamp_column, *id_columns)
            .execution_options(yield_per=self.batch_size)
        )

        result = await self.client.stream_scalars(statement)
        try:
            async for batch in result.partitions(self.batch_size):
                yield list(batch)
        finally:
            await result.close()
