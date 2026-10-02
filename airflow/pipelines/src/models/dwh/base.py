from dataclasses import dataclass, fields
from typing import ClassVar


@dataclass(slots=True, kw_only=True)
class ClickHouseModel:
    # ClassVar не включает в конструктор
    table_name: ClassVar[str]
    distributed_table_name: ClassVar[str | None] = None

    @classmethod
    def column_names(cls) -> tuple[str, ...]:
        return tuple(column.name for column in fields(cls))

    @classmethod
    def table_to_insert(cls) -> str:
        return cls.distributed_table_name or cls.table_name

    def to_row(self) -> tuple[object, ...]:
        return tuple(getattr(self, name) for name in self.column_names())
