from typing import Protocol, TypeVar
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import ClickHouseModel
from src.models.oltp import Base


SourceT = TypeVar("SourceT", bound=Base)
TargetT = TypeVar("TargetT", bound=ClickHouseModel)


class BatchTransformer(Protocol[SourceT, TargetT]):
    @staticmethod
    async def transform(
        batch: list[SourceT],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[TargetT]: ...
