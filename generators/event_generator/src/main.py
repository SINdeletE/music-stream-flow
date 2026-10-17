import logging
import signal
from threading import Event
from time import monotonic

import psycopg
from confluent_kafka import KafkaException
from psycopg.rows import dict_row

from .publisher import KafkaPublisher
from .runtime import EventGenerator
from .settings import EventSettings
from .source import OltpSource

logger = logging.getLogger(__name__)


def connect(settings: EventSettings, stop: Event) -> psycopg.Connection | None:
    deadline = monotonic() + settings.startup_timeout_seconds
    while not stop.is_set():
        try:
            return psycopg.connect(
                **settings.source.connection_kwargs(),
                autocommit=True,
                row_factory=dict_row,
            )
        except psycopg.OperationalError:
            if monotonic() >= deadline:
                raise TimeoutError("PostgreSQL startup deadline exceeded") from None
            logger.warning("Waiting for PostgreSQL")
            stop.wait(min(1, max(0, deadline - monotonic())))
    return None


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    stop = Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    connection = None
    publisher = None
    generator = None
    status = 0
    try:
        settings = EventSettings.from_env()
        connection = connect(settings, stop)
        if connection is not None and not stop.is_set():
            publisher = KafkaPublisher(settings)
            if publisher.wait_ready(stop):
                generator = EventGenerator(OltpSource(connection), publisher, settings)
                logger.info("Event generator started")
                generator.run(stop)
    except (
        ValueError,
        OSError,
        psycopg.Error,
        KafkaException,
        RuntimeError,
        KeyError,
    ) as error:
        logger.error(
            "Event generator stopped: %s; check settings, schema and service availability",
            type(error).__name__,
        )
        status = 1
    finally:
        if publisher is not None:
            try:
                publisher.close()
            except (KafkaException, RuntimeError, TimeoutError) as error:
                logger.error("Kafka shutdown failed: %s", type(error).__name__)
                status = 1
        if connection is not None:
            connection.close()
        if generator is not None:
            logger.info(
                "Event generator stopped; delivered=%s unfinished_sessions=%s",
                generator.total_sent,
                len(generator.engine.sessions),
            )
    return status
