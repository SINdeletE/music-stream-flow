import logging
import signal
from threading import Event
from time import monotonic

import psycopg
from psycopg.rows import dict_row

from .catalogs import Catalogs
from .generator import OltpGenerator
from .repository import Repository
from .scheduler import Scheduler
from .settings import Settings

logger = logging.getLogger(__name__)


def connect(settings: Settings, stop: Event) -> psycopg.Connection | None:
    deadline = monotonic() + settings.startup_timeout_seconds
    while not stop.is_set():
        try:
            return psycopg.connect(
                **settings.connection_kwargs(), autocommit=True, row_factory=dict_row
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
    try:
        settings = Settings.from_env()
        catalogs = Catalogs.load()
        connection = connect(settings, stop)
        if connection is None:
            return 0
        with connection:
            repository = Repository(connection)
            repository.validate_schema()
            logger.info("OLTP generator started")
            Scheduler(
                OltpGenerator(repository, catalogs, seed=settings.random_seed), settings
            ).run(stop)
    except (ValueError, OSError, psycopg.Error, RuntimeError, KeyError) as error:
        logger.error(
            "OLTP generator stopped: %s; check configuration, schema and database availability",
            type(error).__name__,
        )
        return 1
    logger.info("OLTP generator stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
