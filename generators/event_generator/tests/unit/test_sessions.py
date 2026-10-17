from dataclasses import replace
from datetime import timedelta
from random import Random

import pytest

from generators.event_generator.src.entities.generates import SessionEngine, UserPool
from generators.event_generator.src.entities.models import EventType
from generators.event_generator.src.source import SourceSnapshot


@pytest.mark.parametrize(
    "anonymous,likes,dislikes,reaction",
    [
        (100, 100, 0, None),
        (0, 100, 0, EventType.track_liked),
        (0, 0, 100, EventType.track_disliked),
        (0, 0, 0, None),
    ],
)
def test_session_engine_positive_complete_sequence(
    settings_factory, snapshot, clock, anonymous, likes, dislikes, reaction
):
    """[unit] A session has bounded ordered actions, stable context and correctly correlated reactions."""
    engine = SessionEngine(
        settings_factory(
            anonymous_session_percent=anonymous,
            like_percent=likes,
            dislike_percent=dislikes,
        )
    )
    events = []
    for _ in range(20):
        prepared = engine.prepare(snapshot, clock.utcnow())
        engine.confirm(prepared)
        events.append(prepared.event)
        clock.advance(1)
        if prepared.event.event_type == EventType.session_ended:
            break
    expected = [EventType.session_started]
    for _ in range(2):
        expected.append(EventType.track_selected)
        if reaction:
            expected.append(reaction)
    expected.append(EventType.session_ended)
    assert [event.event_type for event in events] == expected
    assert len({event.session_id for event in events}) == 1
    assert len({event.user_id for event in events}) == 1
    assert (
        len(
            {
                (event.device_id, event.device_os_id, event.app_release_id)
                for event in events
            }
        )
        == 1
    )
    assert len({event.event_id for event in events}) == len(events)
    assert all(event.received_at == event.event_timestamp for event in events)
    assert all(event.user_id is None for event in events) == (anonymous == 100)
    assert all(
        event.track_id is None and event.playback_id is None
        for event in (events[0], events[-1])
    )
    current = None
    playbacks = []
    for event in events:
        if event.event_type == EventType.track_selected:
            current = event
            playbacks.append(event.playback_id)
            assert event.track_id in snapshot.track_ids
        elif event.event_type in (EventType.track_liked, EventType.track_disliked):
            assert event.user_id is not None
            assert (event.track_id, event.playback_id) == (
                current.track_id,
                current.playback_id,
            )
    assert len(set(playbacks)) == 2
    assert not engine.sessions
    assert (
        engine.prepare(snapshot, clock.utcnow()).event.session_id
        != events[0].session_id
    )


def test_session_engine_positive_prepare_does_not_advance(
    settings_factory, snapshot, clock
):
    """[unit] Preparing or retrying a pending event never commits unacknowledged state."""
    engine = SessionEngine(settings_factory())
    start = engine.prepare(snapshot, clock.utcnow())
    assert not engine.sessions
    assert engine.prepare(snapshot, clock.utcnow() + timedelta(seconds=10)) is start
    engine.confirm(start)
    before = tuple(engine.sessions)
    selected = engine.prepare(snapshot, clock.utcnow())
    assert tuple(engine.sessions) == before
    engine.confirm(selected)
    assert engine.sessions[0].selected_tracks == 1
    with pytest.raises(ValueError):
        engine.confirm(selected)


def test_session_engine_positive_interleaving_and_unique_installations(
    settings_factory, snapshot, clock
):
    """[unit] Active sessions interleave fairly and never share an installation."""
    engine = SessionEngine(settings_factory(max_active_sessions=10))
    events = []
    for _ in range(6):
        prepared = engine.prepare(snapshot, clock.utcnow())
        engine.confirm(prepared)
        events.append(prepared.event)
    assert [event.event_type for event in events[:3]] == [EventType.session_started] * 3
    assert [event.session_id for event in events[:3]] == [
        event.session_id for event in events[3:]
    ]
    assert len({session.context.installation_id for session in engine.sessions}) == 3


def test_session_engine_positive_active_context_is_frozen(
    settings_factory, snapshot, clock
):
    """[unit] Refreshing OS or app metadata cannot change an already started session's context."""
    engine = SessionEngine(settings_factory())
    prepared = engine.prepare(snapshot, clock.utcnow())
    engine.confirm(prepared)
    changed = SourceSnapshot(
        tuple(
            replace(context, device_os_id=snapshot.contexts[0].installation_id)
            for context in snapshot.contexts
        ),
        snapshot.track_ids,
    )
    selected = engine.prepare(changed, clock.utcnow())
    assert selected.event.device_os_id == prepared.event.device_os_id


@pytest.mark.parametrize(
    "contexts,tracks", [(False, True), (True, False), (False, False)]
)
def test_session_engine_positive_missing_sources_wait(
    settings_factory, snapshot, clock, contexts, tracks
):
    """[unit] Missing sources never create invented references or incomplete new sessions."""
    engine = SessionEngine(settings_factory())
    assert (
        engine.prepare(
            SourceSnapshot(
                snapshot.contexts if contexts else (),
                snapshot.track_ids if tracks else (),
            ),
            clock.utcnow(),
        )
        is None
    )
    assert not engine.sessions


def test_user_pool_positive_create_reuse_and_limit(settings_factory):
    """[unit] User identities can return across sessions without exceeding the in-memory pool."""
    pool = UserPool(
        settings_factory(
            anonymous_session_percent=0, new_user_percent=100, user_pool_max_size=2
        ),
        Random(1),
    )
    first, second = pool.choose(), pool.choose()
    assert first != second
    assert {pool.choose() for _ in range(100)} <= {first, second}
    assert len(pool.users) == 2
    reuse = UserPool(
        settings_factory(anonymous_session_percent=0, new_user_percent=0), Random(1)
    )
    initial = reuse.choose()
    assert all(reuse.choose() == initial for _ in range(20))


def test_user_pool_positive_anonymous_never_creates_users(settings_factory):
    """[unit] Anonymous sessions do not populate the synthetic account pool."""
    pool = UserPool(settings_factory(anonymous_session_percent=100), Random(1))
    assert all(pool.choose() is None for _ in range(20))
    assert pool.users == []


def test_session_engine_positive_user_returns_on_another_device(
    settings_factory, snapshot, clock
):
    """[unit] One pooled user can have sessions on different devices without changing identity."""
    engine = SessionEngine(
        settings_factory(
            max_active_sessions=2, anonymous_session_percent=0, user_pool_max_size=1
        )
    )
    first = engine.prepare(snapshot, clock.utcnow())
    engine.confirm(first)
    second = engine.prepare(snapshot, clock.utcnow())
    engine.confirm(second)
    assert first.event.user_id == second.event.user_id
    assert first.event.device_id != second.event.device_id
