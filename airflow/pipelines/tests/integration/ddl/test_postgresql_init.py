from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from tests.integration.fixtures.postgresql._fixture import (
    DDL_DIRECTORY,
    POSTGRESQL_IMAGE,
)

ROOT = Path(__file__).resolve().parents[5]
LABELS = {"music-stream-flow.test-suite": "integration"}


@pytest.fixture
def database():
    password = uuid4().hex
    with (
        Network(docker_network_kw={"labels": LABELS}) as network,
        (
            DockerContainer(POSTGRESQL_IMAGE)
            .with_network(network)
            .with_network_aliases("music-oltp")
            .with_env("POSTGRES_USER", "init_test")
            .with_env("POSTGRES_PASSWORD", password)
            .with_env("POSTGRES_DB", "init_test")
            .with_kwargs(labels=LABELS)
        ) as container,
    ):
        deadline = monotonic() + 120
        while True:
            code, _ = container.exec(
                ["pg_isready", "-h", "127.0.0.1", "-U", "init_test", "-d", "init_test"]
            )
            if code == 0:
                break
            if monotonic() >= deadline:
                raise TimeoutError("Temporary PostgreSQL did not become ready")
            sleep(0.2)
        yield container, network, password


def run_init(network, password, extra_files=()):
    container = (
        DockerContainer(POSTGRESQL_IMAGE)
        .with_network(network)
        .with_env("PGHOST", "music-oltp")
        .with_env("PGPORT", "5432")
        .with_env("PGUSER", "init_test")
        .with_env("PGPASSWORD", password)
        .with_env("PGDATABASE", "init_test")
        .with_kwargs(labels=LABELS, entrypoint=["/bin/sh", "/init/init.sh"])
        .with_volume_mapping(ROOT / "postgresql/init.sh", "/init/init.sh")
    )
    # Include the manual reset deliberately to check that the numbered glob excludes it.
    for path in (*sorted(DDL_DIRECTORY.glob("*.sql")), *extra_files):
        container.with_volume_mapping(path, f"/init/sql/{path.name}")
    with container:
        result = container.get_wrapped_container().wait(timeout=120)
        stdout, stderr = container.get_logs()
        return result["StatusCode"], (stdout + stderr).decode()


def query(container, statement):
    code, output = container.exec(
        [
            "psql",
            "--no-psqlrc",
            "--username=init_test",
            "--dbname=init_test",
            "--no-align",
            "--tuples-only",
            "--set=ON_ERROR_STOP=1",
            "--command",
            statement,
        ]
    )
    assert code == 0, output.decode()
    return output.decode().strip()


def test_postgresql_init_positive_records_and_rerun(database):
    """[integration] The one-shot init container creates OLTP and preserves data on reruns."""
    container, network, password = database
    code, logs = run_init(network, password)
    assert code == 0, logs
    assert "PostgreSQL schema initialization completed." in logs
    assert (
        query(
            container, "SELECT count(*) FROM pg_tables WHERE schemaname = 'music_oltp'"
        )
        == "12"
    )
    assert (
        query(
            container,
            "SELECT count(*) FROM pg_constraint WHERE connamespace = 'music_oltp'::regnamespace AND contype = 'c'",
        )
        == "5"
    )
    query(
        container,
        "INSERT INTO music_oltp.device (id, name, device_version) "
        "VALUES ('00000000-0000-0000-0000-000000000001', 'Test phone', '1.0')",
    )

    code, logs = run_init(network, password)
    assert code == 0, logs
    assert "drop-all.sql" not in logs
    assert query(container, "SELECT count(*) FROM music_oltp.device") == "1"
    assert (
        query(
            container, "SELECT count(*) FROM pg_tables WHERE schemaname = 'music_oltp'"
        )
        == "12"
    )
    assert (
        query(
            container,
            "SELECT count(*) FROM pg_constraint WHERE connamespace = 'music_oltp'::regnamespace AND contype = 'c'",
        )
        == "5"
    )


def test_postgresql_init_negative_sql_error(database, tmp_path):
    """[integration] SQL failures return a nonzero exit code and stop subsequent scripts."""
    container, network, password = database
    failure = tmp_path / "0004-failure.sql"
    subsequent = tmp_path / "0005-subsequent.sql"
    failure.write_text("SELECT 1 / 0;", encoding="utf-8")
    subsequent.write_text(
        "CREATE TABLE public.should_not_exist (id integer);", encoding="utf-8"
    )

    code, logs = run_init(network, password, (failure, subsequent))
    assert code != 0
    assert "0004-failure.sql" in logs
    assert "0005-subsequent.sql" not in logs
    assert "PostgreSQL schema initialization completed." not in logs
    assert (
        query(container, "SELECT to_regclass('public.should_not_exist') IS NULL") == "t"
    )
