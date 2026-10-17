from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

import pytest

from generators.oltp_generator.generator import OltpGenerator, catalog_id


def prepare(generator, clock):
    for table, count in (
        ("device", 2),
        ("genre", 3),
        ("artist", 3),
        ("app_release", 2),
        ("release", 1),
    ):
        for _ in range(count):
            generator.execute(table, "insert", clock.utcnow())


def test_generator_positive_all_tables_and_independent_junctions(
    generator, repository, clock, monkeypatch
):
    """[unit] Track creation adds links atomically without creating or updating parents."""
    monkeypatch.setattr(generator.random, "randint", lambda minimum, maximum: maximum)
    prepare(generator, clock)
    parent_tables = ("artist", "genre", "device", "release", "app_release")
    before = {table: deepcopy(repository.data[table]) for table in parent_tables}
    assert generator.execute("installation", "insert", clock.utcnow()) is not None
    first = generator.execute("track", "insert", clock.utcnow())
    second = generator.execute("track", "insert", clock.utcnow())
    assert first != second
    assert all(repository.data[table] for table in repository.data)
    assert {table: repository.data[table] for table in parent_tables} == before
    assert repository.count("track") == 2
    assert repository.count("track_artist") == 6
    assert repository.count("track_genre") == 6
    assert [row["track_number"] for row in repository.data["track_release"]] == [1, 2]


@pytest.mark.parametrize("table", ["track", "release", "installation"])
def test_generator_positive_missing_dependencies_defer_without_rows(
    generator, repository, clock, table
):
    """[unit] Missing dependencies defer creation and never produce partial entities."""
    assert generator.execute(table, "insert", clock.utcnow()) is None
    assert not any(repository.data.values())


def test_generator_positive_finite_catalogs_and_restart(
    generator, repository, catalogs, clock
):
    """[unit] Catalogs stop growing after exhaustion and preserve IDs on restart."""
    for table, count in (
        ("device", len(catalogs.devices)),
        ("genre", len(catalogs.genres)),
    ):
        ids = [generator.execute(table, "insert", clock.utcnow()) for _ in range(count)]
        assert all(ids)
        assert generator.execute(table, "insert", clock.utcnow()) is None
        restarted = OltpGenerator(repository, catalogs)
        assert restarted.execute(table, "insert", clock.utcnow()) is None
        assert [row["id"] for row in repository.data[table]] == ids


def test_generator_positive_existing_natural_keys_keep_original_uuid(
    generator, repository, clock
):
    """[unit] Pre-existing catalog rows retain non-deterministic historical UUIDs."""
    existing_id = uuid4()
    repository.insert("genre", {"id": existing_id, "name": "Rock"})
    generator.execute("genre", "insert", clock.utcnow())
    assert [row["id"] for row in repository.rows("genre", name="Rock")] == [existing_id]
    assert repository.rows("genre", name="Jazz")[0]["id"] == catalog_id("genre", "Jazz")


def test_generator_positive_os_history_order_and_exhaustion(
    generator, repository, clock
):
    """[unit] OS upgrades follow explicit compatible version order and never repeat."""
    device_id = generator.execute("device", "insert", clock.utcnow())
    assert repository.count("device_os_history") == 1
    for version in ("14", "15"):
        clock.advance(1)
        assert (
            generator.execute("device_os_history", "insert", clock.utcnow()) is not None
        )
        assert repository.data["device_os_history"][-1]["os_version"] == version
    clock.advance(1)
    assert generator.execute("device_os_history", "insert", clock.utcnow()) is None
    rows = repository.rows("device_os_history", device_id=device_id)
    assert [row["os_version"] for row in rows] == ["13", "14", "15"]
    assert [row["valid_from"] for row in rows] == sorted(
        row["valid_from"] for row in rows
    )


@pytest.mark.parametrize("table", ["artist", "release", "track", "installation"])
def test_generator_positive_updates_preserve_created_at(
    generator, repository, clock, table
):
    """[unit] Updates retain creation time and advance business data and updated_at."""
    prepare(generator, clock)
    if table in ("track", "installation"):
        generator.execute(table, "insert", clock.utcnow())
    row = deepcopy(repository.data[table][0])
    clock.advance(1)
    updated = generator.execute(table, "update", clock.utcnow())
    actual = repository.rows(table, id=updated)[0]
    assert actual["created_at"] == row["created_at"]
    assert actual["updated_at"] == clock.utcnow()
    assert actual != row
    assert (
        generator.execute(
            table,
            "update",
            clock.utcnow(),
            excluded=[item["id"] for item in repository.data[table]],
        )
        is None
    )


def test_generator_negative_link_failure_rolls_back_entire_track(
    generator, repository, clock, monkeypatch
):
    """[unit] A junction failure rolls back both the track and previously created links."""
    prepare(generator, clock)
    before = deepcopy(repository.data)
    original = repository.insert

    def insert(table, row):
        if table == "track_release":
            raise RuntimeError("Synthetic junction failure")
        original(table, row)

    monkeypatch.setattr(repository, "insert", insert)
    with pytest.raises(RuntimeError):
        generator.execute("track", "insert", clock.utcnow())
    assert repository.data == before


def test_generator_negative_non_utcoffset_timestamp(generator, clock):
    """[unit] Naive timestamps are rejected before any write."""
    with pytest.raises(ValueError):
        generator.execute("artist", "insert", clock.utcnow().replace(tzinfo=None))


def test_generator_positive_installation_platform_and_dates(
    generator, repository, catalogs, clock
):
    """[unit] Installations reference matching platforms and chronologically valid releases."""
    prepare(generator, clock)
    clock.advance(10)
    entity_id = generator.execute("installation", "insert", clock.utcnow())
    row = repository.rows("installation", id=entity_id)[0]
    device = repository.rows("device", id=row["device_id"])[0]
    app = repository.rows("app_release", id=row["app_release_id"])[0]
    profile = next(item.os for item in catalogs.devices if item.name == device["name"])
    assert app["platform"] == profile.platform
    assert device["created_at"] <= row["installed_at"]
    assert app["released_at"] <= row["installed_at"]
    assert row["created_at"] <= clock.utcnow() + timedelta(milliseconds=1)
