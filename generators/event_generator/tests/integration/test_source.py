from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest

from generators.event_generator.src.source import (
    OltpSource,
    SessionContext,
    SourceSnapshot,
)

from ._data import database_state, insert, populate


def test_event_source_positive_existing_contexts_and_read_only(database, reader):
    """[integration] Source snapshots use only real linked records and do not modify any OLTP table."""
    _, writer = database
    contexts, tracks = populate(writer)
    before = database_state(writer)
    snapshot = OltpSource(reader).read_snapshot(datetime.now(UTC))
    assert set(snapshot.contexts) == {SessionContext(**context) for context in contexts}
    assert set(snapshot.track_ids) == set(tracks)
    assert database_state(writer) == before
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        reader.execute("DELETE FROM music_oltp.track")
    assert database_state(writer) == before


def test_event_source_positive_empty_database(reader):
    """[integration] An empty database produces an empty snapshot without inventing references."""
    assert OltpSource(reader).read_snapshot(datetime.now(UTC)) == SourceSnapshot()


def test_event_source_positive_latest_os_and_dates(database, reader):
    """[integration] A context selects the latest effective OS, not a future history row."""
    _, writer = database
    contexts, _ = populate(writer, 1)
    now = datetime.now(UTC)
    effective_id = uuid4()
    insert(
        writer,
        "device_os_history",
        id=effective_id,
        device_id=contexts[0]["device_id"],
        os_name="Android",
        os_version="15",
        valid_from=now - timedelta(seconds=1),
        created_at=now - timedelta(seconds=1),
    )
    insert(
        writer,
        "device_os_history",
        id=uuid4(),
        device_id=contexts[0]["device_id"],
        os_name="Android",
        os_version="16",
        valid_from=now + timedelta(days=1),
        created_at=now,
    )
    snapshot = OltpSource(reader).read_snapshot(now)
    assert snapshot.contexts[0].device_os_id == effective_id


@pytest.mark.parametrize(
    "cause",
    [
        "future_installation",
        "future_release",
        "future_device",
        "incompatible_platform",
        "missing_os",
    ],
)
def test_event_source_positive_ineligible_context_is_excluded(database, reader, cause):
    """[integration] Invalid temporal or platform relationships are not emitted as session contexts."""
    _, writer = database
    contexts, _ = populate(writer, 1)
    future = datetime.now(UTC) + timedelta(days=1)
    context = contexts[0]
    if cause == "future_installation":
        writer.execute(
            "UPDATE music_oltp.installation SET installed_at = %s", (future,)
        )
    elif cause == "future_release":
        writer.execute("UPDATE music_oltp.app_release SET released_at = %s", (future,))
    elif cause == "future_device":
        writer.execute("UPDATE music_oltp.device SET created_at = %s", (future,))
    elif cause == "incompatible_platform":
        writer.execute("UPDATE music_oltp.app_release SET platform = 'ios'")
    else:
        writer.execute(
            "DELETE FROM music_oltp.device_os_history WHERE device_id = %s",
            (context["device_id"],),
        )
    assert OltpSource(reader).read_snapshot(datetime.now(UTC)).contexts == ()


@pytest.mark.parametrize("cause", ["unavailable", "deleted", "future"])
def test_event_source_positive_ineligible_track_is_excluded(database, reader, cause):
    """[integration] Unavailable, deleted and future-created tracks are absent from selection pools."""
    _, writer = database
    populate(writer, 1)
    if cause == "unavailable":
        writer.execute("UPDATE music_oltp.track SET is_available = FALSE")
    elif cause == "deleted":
        writer.execute("UPDATE music_oltp.track SET deleted_at = now()")
    else:
        writer.execute(
            "UPDATE music_oltp.track SET created_at = %s",
            (datetime.now(UTC) + timedelta(days=1),),
        )
    assert OltpSource(reader).read_snapshot(datetime.now(UTC)).track_ids == ()


def test_event_source_positive_new_commits_become_visible(database, reader):
    """[integration] A later snapshot sees new committed source data without reconnecting."""
    _, writer = database
    source = OltpSource(reader)
    assert source.read_snapshot(datetime.now(UTC)) == SourceSnapshot()
    contexts, tracks = populate(writer, 1)
    snapshot = source.read_snapshot(datetime.now(UTC))
    assert snapshot.contexts == (SessionContext(**contexts[0]),)
    assert snapshot.track_ids == tuple(tracks)
