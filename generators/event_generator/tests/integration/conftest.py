import shlex
import tarfile
from io import BytesIO
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg.rows import dict_row
from testcontainers.core.container import DockerContainer

from generators.event_generator.src.settings import SourceSettings

DDL_DIRECTORY = Path(__file__).resolve().parents[4] / "sql/music_oltp/ddl"
TABLES = (
    "device",
    "device_os_history",
    "app_release",
    "installation",
    "artist",
    "genre",
    "release",
    "track",
    "track_genre",
    "track_artist",
    "track_release",
    "artist_release",
)


@pytest.fixture(scope="session")
def database_service():
    """Provide isolated PostgreSQL initialized from the unmodified working DDL."""
    container = (
        DockerContainer("postgres:18.3-alpine")
        .with_env("POSTGRES_USER", "event_test")
        .with_env("POSTGRES_PASSWORD", "event-test-password")
        .with_env("POSTGRES_DB", "event_test")
        .with_exposed_ports(5432)
        .with_kwargs(labels={"music-stream-flow.test-suite": "integration"})
    )
    connection = None
    try:
        container.start()
        settings = SourceSettings(
            container.get_container_host_ip(),
            int(container.get_exposed_port(5432)),
            "event_test",
            "event_test",
            "event-test-password",
        )
        kwargs = settings.connection_kwargs()
        kwargs.pop("options")
        deadline = monotonic() + 120
        while True:
            try:
                connection = psycopg.connect(
                    **kwargs, autocommit=True, row_factory=dict_row
                )
                break
            except psycopg.OperationalError:
                if monotonic() >= deadline:
                    raise
                sleep(0.2)
        for path in sorted(DDL_DIRECTORY.glob("*.sql")):
            connection.execute(path.read_text(encoding="utf-8"))
        yield settings, connection
    finally:
        if connection is not None:
            connection.close()
        container.stop()


@pytest.fixture
def database(database_service):
    settings, connection = database_service
    connection.execute(
        "TRUNCATE " + ", ".join(f"music_oltp.{table}" for table in TABLES)
    )
    return settings, connection


@pytest.fixture
def reader(database):
    settings, _ = database
    with psycopg.connect(
        **settings.connection_kwargs(), autocommit=True, row_factory=dict_row
    ) as connection:
        yield connection


@pytest.fixture(scope="session")
def kafka_service():
    """Run Kafka in KRaft mode with a random, externally reachable advertised port."""
    start_script = "event-test-kafka-start.sh"
    container = (
        DockerContainer("apache/kafka:4.3.1")
        .with_kwargs(labels={"music-stream-flow.test-suite": "integration"})
        .with_exposed_ports(9092)
        .with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env(
            "KAFKA_LISTENERS",
            "INTERNAL://0.0.0.0:9094,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        )
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("KAFKA_LOG_DIRS", "/tmp/event-kraft-logs")
        .with_command(
            [
                "/bin/bash",
                "-ec",
                f"while [ ! -f /tmp/{start_script} ]; do sleep 0.1; done; exec /bin/bash /tmp/{start_script}",
            ]
        )
    )
    try:
        container.start()
        bootstrap = (
            f"{container.get_container_host_ip()}:{container.get_exposed_port(9092)}"
        )
        advertised = f"INTERNAL://localhost:9094,EXTERNAL://{bootstrap}"
        script = (
            "#!/bin/bash\nset -e\n"
            + f"export KAFKA_ADVERTISED_LISTENERS={shlex.quote(advertised)}\n"
            + "exec /etc/kafka/docker/run\n"
        ).encode()
        with BytesIO() as archive:
            with tarfile.open(fileobj=archive, mode="w") as tar:
                info = tarfile.TarInfo(start_script)
                info.mode = 0o644
                info.size = len(script)
                tar.addfile(info, BytesIO(script))
            container.get_wrapped_container().put_archive("/tmp", archive.getvalue())
        admin = AdminClient({"bootstrap.servers": bootstrap})
        deadline = monotonic() + 120
        while True:
            try:
                if admin.list_topics(timeout=2).brokers:
                    break
            except KafkaException:
                pass
            if monotonic() >= deadline:
                raise TimeoutError("Temporary Kafka did not become ready")
            sleep(0.2)
        yield bootstrap, admin
    finally:
        container.stop()


@pytest.fixture
def kafka_topic(kafka_service):
    bootstrap, admin = kafka_service
    name = f"event-test-{uuid4().hex}"
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=3, replication_factor=1)]
    )
    futures[name].result(timeout=30)
    return bootstrap, name
