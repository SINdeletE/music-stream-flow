from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from generators.oltp_generator.generator import OltpGenerator
from generators.oltp_generator.repository import TABLES
from generators.oltp_generator.scheduler import Scheduler
from generators.oltp_generator.settings import FlowSettings, Settings

NOW = datetime(2026, 10, 17, 12, tzinfo=UTC)


def populate(generator):
    for table, count in (
        ("device", 2),
        ("genre", 3),
        ("artist", 3),
        ("app_release", 3),
        ("release", 2),
        ("installation", 2),
        ("track", 5),
    ):
        for _ in range(count):
            assert generator.execute(table, "insert", NOW) is not None


def test_oltp_generator_positive_all_tables_and_relationships(
    generator, repository, catalogs
):
    """[integration] All OLTP tables receive valid records and coherent relationships."""
    populate(generator)
    for table in TABLES:
        assert repository.count(table) > 0
    for track in repository.rows("track"):
        assert track["created_at"] == track["updated_at"] == NOW
        assert track["deleted_at"] is None
        assert 60_000 <= track["duration_ms"] <= 480_000
        assert repository.rows("track_artist", track_id=track["id"])
        assert 1 <= len(repository.rows("track_genre", track_id=track["id"])) <= 3
        links = repository.rows("track_release", track_id=track["id"])
        assert len(links) == 1
        release_artists = {
            row["artist_id"]
            for row in repository.rows(
                "artist_release", release_id=links[0]["release_id"]
            )
        }
        assert {
            row["artist_id"]
            for row in repository.rows("track_artist", track_id=track["id"])
        } <= release_artists
    for release in repository.rows("release"):
        links = repository.rows("track_release", release_id=release["id"])
        assert sorted(row["track_number"] for row in links) == list(
            range(1, len(links) + 1)
        )
        credits = repository.rows("artist_release", release_id=release["id"])
        assert sorted(row["artist_number"] for row in credits) == list(
            range(1, len(credits) + 1)
        )
    for installation in repository.rows("installation"):
        device = repository.rows("device", id=installation["device_id"])[0]
        app = repository.rows("app_release", id=installation["app_release_id"])[0]
        profile = next(
            item.os for item in catalogs.devices if item.name == device["name"]
        )
        assert app["platform"] == profile.platform
        assert app["released_at"] <= installation["installed_at"]
        assert device["created_at"] <= installation["installed_at"]


@pytest.mark.parametrize("table", ["artist", "release", "track", "installation"])
def test_oltp_generator_positive_update_created_at_is_immutable(
    generator, repository, table
):
    """[integration] Updates change business fields and updated_at without changing created_at."""
    populate(generator)
    before = {row["id"]: row for row in repository.rows(table)}
    entity_id = generator.execute(table, "update", NOW + timedelta(seconds=1))
    assert entity_id in before
    after = repository.rows(table, id=entity_id)[0]
    assert after["created_at"] == before[entity_id]["created_at"]
    assert after["updated_at"] == NOW + timedelta(seconds=1)
    assert after != before[entity_id]
    if table == "installation":
        assert after["last_seen_at"] == after["updated_at"]


def test_oltp_generator_positive_catalog_restart_and_os_history(
    generator, repository, catalogs
):
    """[integration] Existing UUIDs survive restarts and OS history stops at the final version."""
    for _ in catalogs.devices:
        generator.execute("device", "insert", NOW)
    for _ in catalogs.genres:
        generator.execute("genre", "insert", NOW)
    before_devices = {row["id"] for row in repository.rows("device")}
    before_genres = {row["id"] for row in repository.rows("genre")}
    restarted = OltpGenerator(repository, catalogs)
    assert restarted.execute("device", "insert", NOW) is None
    assert restarted.execute("genre", "insert", NOW) is None
    for index in range(1, 10):
        restarted.execute("device_os_history", "insert", NOW + timedelta(seconds=index))
    assert (
        restarted.execute("device_os_history", "insert", NOW + timedelta(seconds=11))
        is None
    )
    assert repository.count("device_os_history") == 6
    assert {row["id"] for row in repository.rows("device")} == before_devices
    assert {row["id"] for row in repository.rows("genre")} == before_genres
    for row in repository.rows("device"):
        profile = next(
            device.os for device in catalogs.devices if device.name == row["name"]
        )
        history = sorted(
            repository.rows("device_os_history", device_id=row["id"]),
            key=lambda item: item["valid_from"],
        )
        assert [item["os_version"] for item in history] == list(profile.versions)


def test_oltp_generator_positive_existing_catalog_uuid_is_preserved(
    generator, repository
):
    """[integration] Natural-key adoption does not rewrite historical catalog IDs."""
    entity_id = uuid4()
    repository.insert(
        "genre", {"id": entity_id, "name": "Rock", "created_at": NOW, "updated_at": NOW}
    )
    generator.execute("genre", "insert", NOW)
    assert repository.rows("genre", name="Rock")[0]["id"] == entity_id
    assert repository.count("genre") == 2


def test_oltp_generator_positive_existing_device_uuid_is_preserved(
    generator, repository
):
    """[integration] OS history references the adopted historical device UUID."""
    entity_id = uuid4()
    repository.insert(
        "device",
        {
            "id": entity_id,
            "name": "Google Pixel 7",
            "device_version": "1.0",
            "created_at": NOW,
            "updated_at": NOW,
        },
    )
    generator.execute("device", "insert", NOW)
    assert generator.execute("device", "insert", NOW) is None
    assert repository.count("device") == 2
    assert repository.rows("device", name="Google Pixel 7")[0]["id"] == entity_id
    for index in range(1, 10):
        generator.execute("device_os_history", "insert", NOW + timedelta(seconds=index))
    history = repository.rows("device_os_history", device_id=entity_id)
    assert len(history) == 3
    assert {row["os_version"] for row in history} == {"13", "14", "15"}


@pytest.mark.parametrize("table", ["track", "release", "installation"])
def test_oltp_generator_positive_missing_dependencies_do_not_write(
    generator, repository, table
):
    """[integration] An empty dependency set cannot create partial primary records."""
    assert generator.execute(table, "insert", NOW) is None
    assert all(repository.count(name) == 0 for name in TABLES)


def test_oltp_generator_negative_fk_violation_rolls_back_track(
    generator, repository, monkeypatch
):
    """[integration] A real FK violation rolls back the track and every preceding junction."""
    populate(generator)
    before = {table: repository.count(table) for table in TABLES}
    original_insert = repository.insert

    def insert(table, row):
        if table == "track_release":
            row = {**row, "release_id": uuid4()}
        original_insert(table, row)

    monkeypatch.setattr(repository, "insert", insert)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        generator.execute("track", "insert", NOW)
    assert {table: repository.count(table) for table in TABLES} == before


def test_oltp_generator_negative_initial_os_failure_rolls_back_device(
    generator, repository, monkeypatch
):
    """[integration] Failure to create initial OS history rolls back its device."""
    original_insert = repository.insert

    def insert(table, row):
        if table == "device_os_history":
            row = {**row, "device_id": uuid4()}
        original_insert(table, row)

    monkeypatch.setattr(repository, "insert", insert)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        generator.execute("device", "insert", NOW)
    assert repository.count("device") == 0
    assert repository.count("device_os_history") == 0


def test_oltp_generator_positive_scheduler_limits_and_junctions(generator, repository):
    """[integration] Real commits spend only primary entity quotas, not junction quotas."""
    flows = tuple(
        FlowSettings(table, "insert", 60000, 100, max_per_period=limit)
        for table, limit in (("genre", 3), ("artist", 3), ("release", 1), ("track", 5))
    )
    flows += (
        FlowSettings("artist", "update", 60000, 100, percent_per_period=Decimal(100)),
    )
    settings = Settings("unused", 5432, "unused", "unused", "unused", flows=flows)
    elapsed = [0.0]
    scheduler = Scheduler(
        generator,
        settings,
        clock=lambda: elapsed[0],
        utcnow=lambda: NOW + timedelta(seconds=elapsed[0]),
    )
    for index in range(10):
        elapsed[0] = index / 5
        scheduler.step()
    assert repository.count("track") == 5
    assert repository.count("artist") == 3
    assert repository.count("genre") == 3
    assert repository.count("release") == 1
    assert repository.count("track_release") == 5
    assert repository.count("track_genre") >= 5
    assert [flow.completed for flow in scheduler.flows] == [3, 3, 1, 5, 0]
