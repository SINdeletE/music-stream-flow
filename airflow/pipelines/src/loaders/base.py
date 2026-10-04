from collections.abc import Sequence
from typing import Generic, TypeVar

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import ClickHouseModel


ModelT = TypeVar("ModelT", bound=ClickHouseModel)


class ClickHouseLoader(Generic[ModelT]):
    model: type[ModelT]
    generated_columns: tuple[str, ...] = ()

    def __init__(self, client: AsyncClient) -> None:
        self.client = client

    async def load_batch(self, batch: Sequence[ModelT]) -> int:
        model = type(self).model
        all_columns = model.column_names()
        generated_columns = type(self).generated_columns
        groups: dict[tuple[str, ...], list[tuple[object, ...]]] = {}

        for row in batch:
            if not isinstance(row, model):
                raise TypeError(f"Expected {model.__name__}, got {type(row).__name__}")

            columns = tuple(
                name
                for name in all_columns
                if name not in generated_columns or getattr(row, name) is not None
            )
            values = tuple(getattr(row, name) for name in columns)
            groups.setdefault(columns, []).append(values)

        rows_written = 0
        for columns, data in groups.items():
            await self.client.insert(
                table=model.table_to_insert(),
                data=data,
                column_names=columns,
            )
            rows_written += len(data)

        return rows_written
