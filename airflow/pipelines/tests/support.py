from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.query import QueryResult
from sqlalchemy.ext.asyncio import AsyncSession

from src.connections import DwhSettings, OltpSettings
from src import pipelines


START = datetime(2026, 10, 5, tzinfo=UTC)
END = datetime(2026, 10, 6, tzinfo=UTC)
PIPELINES = [getattr(pipelines, name) for name in pipelines.__all__]
DIM_PIPELINES = [pipeline for pipeline in PIPELINES if pipeline.__name__.startswith("Dim")]
BRG_PIPELINES = [pipeline for pipeline in PIPELINES if pipeline.__name__.startswith("Brg")]


def make_source(model, index=0):
    values = {}
    for position, column in enumerate(model.__table__.columns):
        if column.nullable:
            value = None
        else:
            value = {
                UUID: UUID(int=(index + 1) * 100 + position + 1),
                datetime: START,
                str: f"value-{index}",
                int: index + 1,
                bool: True,
            }[column.type.python_type]
        values[column.name] = value
    if "id" in values:
        values["id"] = UUID(int=index + 1)
    return model(**values)


def snapshot(batch):
    return [
        {column.name: getattr(entity, column.name) for column in entity.__table__.columns}
        for entity in batch
    ]


def oltp_settings():
    return OltpSettings(
        _env_file=None,
        music_oltp_db_user="test_user",
        music_oltp_db_password="test_password",
        music_oltp_db_name="music_oltp",
        music_oltp_db_host="postgres",
        music_oltp_db_port=5432,
        music_oltp_batch_size=2,
    )


def dwh_settings():
    return DwhSettings(
        _env_file=None,
        music_dwh_db_user="test_user",
        music_dwh_db_password="test_password",
        music_dwh_db_name="music_dwh",
        music_dwh_db_host="clickhouse",
        music_dwh_db_port=8123,
    )


class Harness:
    def __init__(self, batches):
        self.batches = batches
        self.events = []
        self.inserts = []
        self.query_error = None
        self.stream_error = None
        self.partition_error = None
        self.oltp_error = None
        self.dwh_error = None
        self.failed_states = set()
        self.data_insert_error = None
        self.client = AsyncMock(spec=AsyncClient)
        self.client.query.side_effect = self.query
        self.client.insert.side_effect = self.insert
        self.session = AsyncMock(spec=AsyncSession)
        self.session.stream_scalars.side_effect = self.stream_scalars
        self.result = SimpleNamespace(
            partitions=self.partitions,
            close=AsyncMock(side_effect=lambda: self.events.append("result_close")),
        )

    @asynccontextmanager
    async def dwh_context(self, settings):
        self.events.append("dwh_open")
        self.received_dwh_settings = settings
        if self.dwh_error:
            raise self.dwh_error
        try:
            yield self.client
        finally:
            await self.client.close()
            self.events.append("dwh_close")

    @asynccontextmanager
    async def oltp_context(self, settings):
        self.events.append("oltp_open")
        self.received_oltp_settings = settings
        if self.oltp_error:
            raise self.oltp_error
        try:
            yield SimpleNamespace(session=self.session_context)
        finally:
            self.events.append("oltp_close")

    @asynccontextmanager
    async def session_context(self):
        self.events.append("session_open")
        try:
            yield self.session
        finally:
            await self.session.close()
            self.events.append("session_close")

    async def stream_scalars(self, statement):
        self.events.append("extract")
        self.statement = statement
        if self.stream_error:
            raise self.stream_error
        return self.result

    async def partitions(self, size):
        self.partition_size = size
        for index, batch in enumerate(self.batches):
            self.events.append(f"batch:{index}")
            if index == 1 and self.partition_error:
                raise self.partition_error
            yield batch

    async def query(self, *, query, parameters):
        self.events.append("query")
        if self.query_error:
            raise self.query_error
        ids = next(iter(parameters.values()))
        return QueryResult(result_set=[(key, 0 if index == 0 else key.int) for index, key in enumerate(ids)])

    async def insert(self, *, table, data, column_names):
        rows = [dict(zip(column_names, row, strict=True)) for row in data]
        self.inserts.append((table, rows))
        self.events.append(f"insert:{table}")
        if table == "music_dwh.etl_loads":
            if rows[0]["state"] in self.failed_states:
                raise RuntimeError(f"audit {rows[0]['state']} failed")
        elif self.data_insert_error:
            raise self.data_insert_error

    @property
    def audits(self):
        return [rows[0] for table, rows in self.inserts if table == "music_dwh.etl_loads"]
