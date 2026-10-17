from decimal import Decimal
from threading import Event

import pytest

from generators.oltp_generator.scheduler import Scheduler
from generators.oltp_generator.settings import FlowSettings


def insertion(table, limit=2, period=1000, interval=100):
    return FlowSettings(table, "insert", period, interval, max_per_period=limit)


def updating(table, percent=100, period=1000, interval=100):
    return FlowSettings(
        table, "update", period, interval, percent_per_period=Decimal(percent)
    )


def scheduler_for(generator, settings_factory, clock, *flows):
    return Scheduler(
        generator, settings_factory(*flows), clock=clock.monotonic, utcnow=clock.utcnow
    )


def test_scheduler_positive_period_limit_and_spacing(
    generator, repository, settings_factory, clock
):
    """[unit] Insert limits reset each period and operations respect their interval."""
    scheduler = scheduler_for(generator, settings_factory, clock, insertion("artist"))
    assert scheduler.step() == 1
    clock.advance(0.05)
    assert scheduler.step() == 0
    clock.advance(0.05)
    assert scheduler.step() == 1
    clock.advance(0.4)
    assert scheduler.step() == 0
    clock.advance(0.5)
    assert scheduler.step() == 1
    assert repository.count("artist") == 3
    assert scheduler.flows[0].completed == 1


def test_scheduler_positive_independent_periods(
    generator, repository, settings_factory, clock
):
    """[unit] Independent table schedules do not inherit another table's exhausted budget."""
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("artist", 1, 1000),
        insertion("genre", 1, 2000),
    )
    assert scheduler.step() == 2
    clock.advance(1)
    assert scheduler.step() == 1
    assert repository.count("artist") == 2
    assert repository.count("genre") == 1
    clock.advance(1)
    assert scheduler.step() == 2


def test_scheduler_positive_no_catchup_burst(
    generator, repository, settings_factory, clock
):
    """[unit] Missed periods and intervals never result in catch-up bursts."""
    scheduler = scheduler_for(
        generator, settings_factory, clock, insertion("artist", 100)
    )
    scheduler.step()
    clock.advance(20)
    assert scheduler.step() == 1
    assert scheduler.step() == 0
    assert repository.count("artist") == 2
    assert scheduler.flows[0].period_start == 20


def test_scheduler_positive_zero_limits(generator, repository, settings_factory, clock):
    """[unit] Disabled flows never create rows or query update baselines."""
    scheduler = scheduler_for(
        generator, settings_factory, clock, insertion("artist", 0), updating("track", 0)
    )
    assert scheduler.flows == []
    assert scheduler.step() == 0
    assert not any(repository.data.values())


def test_scheduler_positive_zero_baseline_does_not_grow_after_insert(
    generator, repository, settings_factory, clock
):
    """[unit] An empty period baseline yields zero updates despite subsequent inserts."""
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("artist", 2),
        updating("artist", 100),
    )
    scheduler.step()
    assert scheduler.flows[1].budget == 0
    clock.advance(0.2)
    scheduler.step()
    assert scheduler.flows[1].completed == 0
    assert repository.count("artist") == 2


def test_scheduler_positive_updates_include_new_rows_and_exclude_repeats(
    generator, repository, settings_factory, clock, monkeypatch
):
    """[unit] The frozen update budget can select new rows but never the same ID twice."""
    for _ in range(4):
        generator.execute("artist", "insert", clock.utcnow())
    clock.advance(1)

    def newest(table, limit=1, *, excluded=()):
        return [
            dict(row)
            for row in reversed(repository.data[table])
            if row["id"] not in excluded
        ][:limit]

    monkeypatch.setattr(repository, "sample", newest)
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("artist", 1, 10000),
        updating("artist", 100, 10000),
    )
    scheduler.step()
    flow = scheduler.flows[1]
    assert flow.budget == 4
    new_id = repository.data["artist"][-1]["id"]
    assert flow.completed == 0
    for _ in range(6):
        clock.advance(0.5)
        scheduler.step()
    assert flow.completed == 4
    assert new_id in flow.updated_ids
    assert len(flow.updated_ids) == 4
    clock.advance(7)
    scheduler.step()
    assert flow.budget == 5
    assert flow.completed <= 1


def test_scheduler_positive_update_budget_rounds_down(
    generator, settings_factory, clock
):
    """[unit] Fractional percentage budgets round down from the pre-insert row count."""
    for _ in range(9):
        generator.execute("artist", "insert", clock.utcnow())
    clock.advance(1)
    scheduler = scheduler_for(
        generator, settings_factory, clock, updating("artist", "12.5")
    )
    scheduler.step()
    assert scheduler.flows[0].budget == 1


def test_scheduler_positive_all_baselines_precede_any_insert(
    generator, settings_factory, clock, monkeypatch
):
    """[unit] All due update baselines are captured before any scheduled write."""
    events = []
    original_count = generator.count
    original_execute = generator.execute

    def count(table):
        events.append(("count", table))
        return original_count(table)

    def execute(table, *args, **kwargs):
        events.append(("execute", table))
        return original_execute(table, *args, **kwargs)

    monkeypatch.setattr(generator, "count", count)
    monkeypatch.setattr(generator, "execute", execute)
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("artist"),
        updating("artist"),
        updating("track"),
    )
    scheduler.step()
    assert events[:2] == [("count", "artist"), ("count", "track")]
    assert events[2] == ("execute", "artist")


def test_scheduler_positive_junctions_do_not_use_parent_budgets(
    generator, repository, settings_factory, clock, monkeypatch
):
    """[unit] Exhausted parent budgets still allow tracks with multiple automatic links."""
    monkeypatch.setattr(generator.random, "randint", lambda minimum, maximum: maximum)
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("genre", 3),
        insertion("artist", 3),
        insertion("release", 1),
        insertion("track", 3),
    )
    for _ in range(3):
        scheduler.step()
        clock.advance(0.2)
    scheduler.step()
    assert repository.count("artist") == 3
    assert repository.count("genre") == 3
    assert repository.count("track") == 3
    assert [flow.completed for flow in scheduler.flows] == [3, 3, 1, 3]
    parents = repository.rows("artist")
    assert repository.count("track_artist") == 3
    assert repository.count("track_genre") == 6
    assert repository.rows("artist") == parents


def test_scheduler_positive_deferred_dependencies_preserve_budget(
    generator, settings_factory, clock
):
    """[unit] A missing dependency consumes no quota and retries on a later interval."""
    scheduler = scheduler_for(generator, settings_factory, clock, insertion("track"))
    assert scheduler.step() == 0
    flow = scheduler.flows[0]
    assert flow.completed == 0
    assert flow.next_operation_at > clock.monotonic()
    for table in ("artist", "release", "genre"):
        generator.execute(table, "insert", clock.utcnow())
    clock.advance(0.2)
    assert scheduler.step() == 1
    assert flow.completed == 1


def test_scheduler_negative_failed_write_does_not_spend_quota(
    generator, settings_factory, clock, monkeypatch
):
    """[unit] A failed transaction leaves counters unchanged and propagates the error."""
    scheduler = scheduler_for(generator, settings_factory, clock, insertion("artist"))

    def fail(*args, **kwargs):
        raise RuntimeError("Synthetic transaction failure")

    monkeypatch.setattr(generator, "execute", fail)
    with pytest.raises(RuntimeError):
        scheduler.step()
    assert scheduler.flows[0].completed == 0


def test_scheduler_positive_shutdown_before_write(
    generator, repository, settings_factory, clock
):
    """[unit] Shutdown prevents subsequent writes and leaves quotas untouched."""
    scheduler = scheduler_for(generator, settings_factory, clock, insertion("artist"))
    stop = Event()
    stop.set()
    assert scheduler.step(stop) == 0
    assert not repository.data["artist"]


def test_scheduler_positive_dependencies_sorted_before_tracks(
    generator, repository, settings_factory, clock
):
    """[unit] Dependent inserts run after their parents even if configured in reverse order."""
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("track", 1),
        insertion("release", 1),
        insertion("artist", 1),
        insertion("genre", 1),
    )
    assert scheduler.step() == 4
    assert repository.count("track") == 1


def test_scheduler_positive_slow_operation_defers_expired_period(
    generator, repository, settings_factory, clock, monkeypatch
):
    """[unit] A slow operation cannot spend another flow's expired period budget."""
    original = generator.execute

    def execute(table, *args, **kwargs):
        result = original(table, *args, **kwargs)
        if table == "genre":
            clock.advance(1.1)
        return result

    monkeypatch.setattr(generator, "execute", execute)
    scheduler = scheduler_for(
        generator,
        settings_factory,
        clock,
        insertion("genre", 1),
        insertion("artist", 1),
    )
    assert scheduler.step() == 1
    assert repository.count("artist") == 0
    assert scheduler.step() == 1
    assert repository.count("artist") == 1
    assert scheduler.flows[1].period_start == 1
