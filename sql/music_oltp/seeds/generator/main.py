import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from seed_generator import (
    DEFAULT_APP_RELEASE_COUNT,
    DEFAULT_ARTIST_COUNT,
    DEFAULT_DEVICE_COUNT,
    DEFAULT_GENRE_COUNT,
    DEFAULT_INSTALLATION_COUNT,
    DEFAULT_RANDOM_SEED,
    DEFAULT_RELEASE_COUNT,
    DEFAULT_TRACK_COUNT,
    MusicOltpSeedGenerator,
    SeedCounts,
    SeedData,
)


PROJECT_ROOT = Path(__file__).resolve().parents[4]
ENV_FILE = PROJECT_ROOT / ".env"

ENV_HOST = "MUSIC_OLTP_SEEDS_GENERATOR_HOST"
ENV_PORT = "MUSIC_OLTP_SEEDS_GENERATOR_PORT"
ENV_DATABASE = "MUSIC_OLTP_SEEDS_GENERATOR_DATABASE"
ENV_USERNAME = "MUSIC_OLTP_SEEDS_GENERATOR_USERNAME"
ENV_PASSWORD = "MUSIC_OLTP_SEEDS_GENERATOR_PASSWORD"
ENV_RANDOM_SEED = "MUSIC_OLTP_SEEDS_GENERATOR_RANDOM_SEED"
ENV_DEVICE_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_DEVICE_COUNT"
ENV_APP_RELEASE_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_APP_RELEASE_COUNT"
ENV_INSTALLATION_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_INSTALLATION_COUNT"
ENV_GENRE_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_GENRE_COUNT"
ENV_ARTIST_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_ARTIST_COUNT"
ENV_RELEASE_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_RELEASE_COUNT"
ENV_TRACK_COUNT = "MUSIC_OLTP_SEEDS_GENERATOR_TRACK_COUNT"
ENV_INTERVAL_MS = "MUSIC_OLTP_SEEDS_GENERATOR_INTERVAL_MS"

DEFAULT_INTERVAL_MS = 10_000

INSERT_STATEMENTS = {
    "device": """
        INSERT INTO music_oltp.device (
            id, name, device_version, os_name, os_version, created_at, updated_at
        ) VALUES (
            %(id)s, %(name)s, %(device_version)s, %(os_name)s, %(os_version)s,
            %(created_at)s, %(updated_at)s
        )
    """,
    "app_release": """
        INSERT INTO music_oltp.app_release (
            id, platform, app_release_version, build, released_at, created_at
        ) VALUES (
            %(id)s, %(platform)s, %(app_release_version)s, %(build)s,
            %(released_at)s, %(created_at)s
        )
    """,
    "installation": """
        INSERT INTO music_oltp.installation (
            id, device_id, app_release_id, installed_at, created_at, updated_at
        ) VALUES (
            %(id)s, %(device_id)s, %(app_release_id)s, %(installed_at)s,
            %(created_at)s, %(updated_at)s
        )
    """,
    "track": """
        INSERT INTO music_oltp.track (
            id, name, duration_ms, image_url, is_available,
            created_at, updated_at, deleted_at
        ) VALUES (
            %(id)s, %(name)s, %(duration_ms)s, %(image_url)s, %(is_available)s,
            %(created_at)s, %(updated_at)s, %(deleted_at)s
        )
    """,
    "genre": """
        INSERT INTO music_oltp.genre (
            id, name, created_at, updated_at, deleted_at
        ) VALUES (
            %(id)s, %(name)s, %(created_at)s, %(updated_at)s, %(deleted_at)s
        )
    """,
    "artist": """
        INSERT INTO music_oltp.artist (
            id, nickname, name, middle_name, last_name, country_code,
            biography, image_url, is_verified, created_at, updated_at, deleted_at
        ) VALUES (
            %(id)s, %(nickname)s, %(name)s, %(middle_name)s, %(last_name)s,
            %(country_code)s, %(biography)s, %(image_url)s, %(is_verified)s,
            %(created_at)s, %(updated_at)s, %(deleted_at)s
        )
    """,
    "release": """
        INSERT INTO music_oltp.release (
            id, name, is_available, released_at, created_at, updated_at, deleted_at
        ) VALUES (
            %(id)s, %(name)s, %(is_available)s, %(released_at)s,
            %(created_at)s, %(updated_at)s, %(deleted_at)s
        )
    """,
    "track_genre": """
        INSERT INTO music_oltp.track_genre (track_id, genre_id, created_at)
        VALUES (%(track_id)s, %(genre_id)s, %(created_at)s)
    """,
    "track_artist": """
        INSERT INTO music_oltp.track_artist (
            track_id, artist_id, artist_number, created_at, updated_at
        ) VALUES (
            %(track_id)s, %(artist_id)s, %(artist_number)s,
            %(created_at)s, %(updated_at)s
        )
    """,
    "track_release": """
        INSERT INTO music_oltp.track_release (
            track_id, release_id, track_number, disc_number, created_at, updated_at
        ) VALUES (
            %(track_id)s, %(release_id)s, %(track_number)s, %(disc_number)s,
            %(created_at)s, %(updated_at)s
        )
    """,
    "artist_release": """
        INSERT INTO music_oltp.artist_release (
            artist_id, release_id, artist_number, created_at, updated_at
        ) VALUES (
            %(artist_id)s, %(release_id)s, %(artist_number)s,
            %(created_at)s, %(updated_at)s
        )
    """,
}


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)

    def connection_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "connect_timeout": 10,
            "application_name": "music_oltp_seed_generator",
        }


def load_database_config() -> DatabaseConfig:
    load_dotenv(ENV_FILE, override=False)

    variable_names = (
        ENV_HOST,
        ENV_PORT,
        ENV_DATABASE,
        ENV_USERNAME,
        ENV_PASSWORD,
    )
    values = {name: os.getenv(name, "").strip() for name in variable_names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ValueError(
            "missing database settings: "
            + ", ".join(missing)
            + f"; fill the seeds generator section in {ENV_FILE}",
        )

    try:
        port = int(values[ENV_PORT])
    except ValueError as error:
        raise ValueError(f"{ENV_PORT} must be an integer") from error

    if not 1 <= port <= 65_535:
        raise ValueError(f"{ENV_PORT} must be between 1 and 65535")

    return DatabaseConfig(
        host=values[ENV_HOST],
        port=port,
        database=values[ENV_DATABASE],
        username=values[ENV_USERNAME],
        password=values[ENV_PASSWORD],
    )


def insert_seed_data(config: DatabaseConfig, data: SeedData) -> None:
    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError("psycopg is not installed; run `make setup`") from error

    try:
        with psycopg.connect(**config.connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                for table_name, rows in data.tables():
                    if rows:
                        cursor.executemany(INSERT_STATEMENTS[table_name], rows)
    except psycopg.Error as error:
        raise RuntimeError(f"database error: {error}") from error


def get_integer_setting(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default

    try:
        return int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and insert Faker seed data into music_oltp.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=get_integer_setting(ENV_RANDOM_SEED, DEFAULT_RANDOM_SEED),
    )
    parser.add_argument(
        "--devices",
        type=int,
        default=get_integer_setting(ENV_DEVICE_COUNT, DEFAULT_DEVICE_COUNT),
    )
    parser.add_argument(
        "--app-releases",
        type=int,
        default=get_integer_setting(
            ENV_APP_RELEASE_COUNT,
            DEFAULT_APP_RELEASE_COUNT,
        ),
    )
    parser.add_argument(
        "--installations",
        type=int,
        default=get_integer_setting(
            ENV_INSTALLATION_COUNT,
            DEFAULT_INSTALLATION_COUNT,
        ),
    )
    parser.add_argument(
        "--genres",
        type=int,
        default=get_integer_setting(ENV_GENRE_COUNT, DEFAULT_GENRE_COUNT),
    )
    parser.add_argument(
        "--artists",
        type=int,
        default=get_integer_setting(ENV_ARTIST_COUNT, DEFAULT_ARTIST_COUNT),
    )
    parser.add_argument(
        "--releases",
        type=int,
        default=get_integer_setting(ENV_RELEASE_COUNT, DEFAULT_RELEASE_COUNT),
    )
    parser.add_argument(
        "--tracks",
        type=int,
        default=get_integer_setting(ENV_TRACK_COUNT, DEFAULT_TRACK_COUNT),
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=get_integer_setting(ENV_INTERVAL_MS, DEFAULT_INTERVAL_MS),
        help="delay between generated batches in milliseconds",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="generate and insert only one batch",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="generate and validate data without connecting to PostgreSQL",
    )
    return parser


def print_summary(data: SeedData, *, inserted: bool, batch_number: int) -> None:
    action = "inserted" if inserted else "generated"
    print(f"Seed batch #{batch_number} {action}:")
    for table_name, rows in data.tables():
        print(f"  {table_name:<16} {len(rows):>6}")


def main() -> int:
    load_dotenv(ENV_FILE, override=False)
    try:
        args = build_parser().parse_args()
        counts = SeedCounts(
            devices=args.devices,
            app_releases=args.app_releases,
            installations=args.installations,
            genres=args.genres,
            artists=args.artists,
            releases=args.releases,
            tracks=args.tracks,
        )
        counts.validate()
        if args.interval_ms <= 0:
            raise ValueError(f"{ENV_INTERVAL_MS} must be greater than zero")

        config = None if args.dry_run else load_database_config()
        batch_number = 1

        while True:
            data = MusicOltpSeedGenerator(
                seed=args.seed + batch_number - 1,
            ).generate(counts)

            if config is not None:
                insert_seed_data(config, data)

            print_summary(
                data,
                inserted=config is not None,
                batch_number=batch_number,
            )

            if args.dry_run or args.once:
                return 0

            print(f"Next batch in {args.interval_ms} ms. Press Ctrl+C to stop.")
            time.sleep(args.interval_ms / 1_000)
            batch_number += 1
    except KeyboardInterrupt:
        print("\nSeed generator stopped.")
        return 0
    except (RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
