import asyncio
import shlex
import tarfile
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from io import BytesIO

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from testcontainers.core.container import DockerContainer

KAFKA_IMAGE = "apache/kafka:4.3.1"
START_SCRIPT = "pipeline-kafka-start.sh"


def _start_kafka(container: DockerContainer) -> str:
    container.start()
    host = container.get_container_host_ip()
    port = container.get_exposed_port(9092)
    bootstrap_servers = f"{host}:{port}"
    listeners = f"INTERNAL://localhost:9094,EXTERNAL://{bootstrap_servers}"
    script = (
        "#!/bin/bash\n"
        "set -e\n"
        f"export KAFKA_ADVERTISED_LISTENERS={shlex.quote(listeners)}\n"
        "exec /etc/kafka/docker/run\n"
    ).encode()
    with BytesIO() as archive:
        with tarfile.open(fileobj=archive, mode="w") as tar:
            info = tarfile.TarInfo(START_SCRIPT)
            info.size = len(script)
            info.mode = 0o644
            tar.addfile(info, BytesIO(script))
        container.get_wrapped_container().put_archive("/tmp", archive.getvalue())

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    deadline = time.monotonic() + 120
    last_error = None
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            if admin.list_topics(timeout=min(2, remaining)).brokers:
                return bootstrap_servers
        except KafkaException as exc:
            last_error = exc
        time.sleep(min(0.2, max(0, deadline - time.monotonic())))
    raise TimeoutError("Kafka was not ready within 120s") from last_error


@asynccontextmanager
async def kafka_bootstrap_servers() -> AsyncGenerator[str, None]:
    """Yield a reachable address of a temporary single-node KRaft broker."""
    container = (
        DockerContainer(KAFKA_IMAGE)
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
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("KAFKA_NUM_PARTITIONS", "3")
        .with_env("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
        .with_command(
            [
                "/bin/bash",
                "-ec",
                (
                    f"while [ ! -f /tmp/{START_SCRIPT} ]; do sleep 0.1; done; "
                    f"exec /bin/bash /tmp/{START_SCRIPT}"
                ),
            ]
        )
    )
    try:
        bootstrap_servers = await asyncio.to_thread(_start_kafka, container)
        yield bootstrap_servers
    finally:
        await asyncio.to_thread(container.stop)
