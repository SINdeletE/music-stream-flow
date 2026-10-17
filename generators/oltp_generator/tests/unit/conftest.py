from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from generators.oltp_generator.catalogs import Catalogs, Device, OsProfile
from generators.oltp_generator.generator import OltpGenerator
from generators.oltp_generator.repository import TABLES
from generators.oltp_generator.settings import Settings


class MemoryRepository:
    def __init__(self):
        self.data = {table: [] for table in TABLES}

    @contextmanager
    def transaction(self):
        before = deepcopy(self.data)
        try:
            yield
        except BaseException:
            self.data = before
            raise

    def count(self, table):
        return len(self.data[table])

    def rows(self, table, **filters):
        return [
            deepcopy(row)
            for row in self.data[table]
            if all(row[key] == value for key, value in filters.items())
        ]

    def sample(self, table, limit=1, *, excluded=()):
        return [deepcopy(row) for row in self.data[table] if row["id"] not in excluded][
            :limit
        ]

    def insert(self, table, row):
        if "id" in row:
            assert not any(item["id"] == row["id"] for item in self.data[table])
        self.data[table].append(dict(row))

    def update(self, table, entity_id, values):
        row = next(row for row in self.data[table] if row["id"] == entity_id)
        row.update(values)

    def next_track_number(self, release_id):
        return 1 + max(
            (
                row["track_number"]
                for row in self.data["track_release"]
                if row["release_id"] == release_id
            ),
            default=0,
        )


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def utcnow(self):
        return datetime(2026, 10, 17, tzinfo=UTC) + timedelta(seconds=self.value)

    def advance(self, seconds):
        self.value += seconds


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def repository():
    return MemoryRepository()


@pytest.fixture
def catalogs():
    return Catalogs(
        devices=(
            Device(
                "Google Pixel 7",
                "1.0",
                OsProfile("Android", "android", ("13", "14", "15")),
            ),
            Device("iPhone 15", "1.0", OsProfile("iOS", "ios", ("17", "18", "26"))),
        ),
        genres=("Rock", "Jazz", "House"),
    )


@pytest.fixture
def generator(repository, catalogs):
    return OltpGenerator(repository, catalogs)


@pytest.fixture
def settings_factory():
    def make(*flows):
        return Settings(
            "localhost", 5432, "unit", "unit", "not-a-real-password", flows=flows
        )

    return make
