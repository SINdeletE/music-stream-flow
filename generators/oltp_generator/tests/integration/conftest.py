from pathlib import Path
from time import monotonic, sleep

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from testcontainers.core.container import DockerContainer

from generators.oltp_generator.catalogs import Catalogs, Device, OsProfile
from generators.oltp_generator.generator import OltpGenerator
from generators.oltp_generator.repository import TABLES, Repository

DDL_DIRECTORY = Path(__file__).resolve().parents[4] / "sql/music_oltp/ddl"


@pytest.fixture(scope="session")
def postgresql_connection():
    """Provide one temporary PostgreSQL; never read project connection settings."""
    container = (
        DockerContainer("postgres:18.3-alpine")
        .with_env("POSTGRES_USER", "generator_test")
        .with_env("POSTGRES_PASSWORD", "generator-test-password")
        .with_env("POSTGRES_DB", "generator_test")
        .with_exposed_ports(5432)
        .with_kwargs(labels={"music-stream-flow.test-suite": "integration"})
    )
    connection = None
    try:
        container.start()
        deadline = monotonic() + 120
        while True:
            try:
                connection = psycopg.connect(
                    host=container.get_container_host_ip(),
                    port=int(container.get_exposed_port(5432)),
                    user="generator_test",
                    password="generator-test-password",
                    dbname="generator_test",
                    connect_timeout=2,
                    autocommit=True,
                    row_factory=dict_row,
                )
                break
            except psycopg.OperationalError:
                if monotonic() >= deadline:
                    raise
                sleep(0.2)
        for path in sorted(DDL_DIRECTORY.glob("*.sql")):
            connection.execute(path.read_text(encoding="utf-8"))
        yield connection
    finally:
        if connection is not None:
            connection.close()
        container.stop()


@pytest.fixture
def repository(postgresql_connection):
    tables = sql.SQL(", ").join(Repository.table(table) for table in sorted(TABLES))
    postgresql_connection.execute(sql.SQL("TRUNCATE {}").format(tables))
    repository = Repository(postgresql_connection)
    repository.validate_schema()
    return repository


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
