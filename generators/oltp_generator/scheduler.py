import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Event
from time import monotonic
from uuid import UUID

from .generator import OltpGenerator
from .settings import INSERT_DEFAULTS, FlowSettings, Settings

logger = logging.getLogger(__name__)


@dataclass
class Flow:
    settings: FlowSettings
    period_start: float | None = None
    next_operation_at: float = 0
    budget: int = 0
    completed: int = 0
    updated_ids: set[UUID] = field(default_factory=set)

    def starts_period(self, now: float) -> bool:
        return (
            self.period_start is None
            or now >= self.period_start + self.settings.period_ms / 1000
        )

    def reset(self, now: float, count: int = 0) -> None:
        period = self.settings.period_ms / 1000
        if self.period_start is None:
            self.period_start = now
        else:
            self.period_start += (
                max(1, int((now - self.period_start) / period)) * period
            )
        self.budget = (
            self.settings.max_per_period
            if self.settings.operation == "insert"
            else int(count * self.settings.percent_per_period / 100)
        )
        self.completed = 0
        self.updated_ids.clear()
        self.next_operation_at = max(now, self.next_operation_at)


class Scheduler:
    def __init__(
        self,
        generator: OltpGenerator,
        settings: Settings,
        *,
        clock: Callable[[], float] = monotonic,
        utcnow: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.generator = generator
        priority = {table: index for index, table in enumerate(INSERT_DEFAULTS)}
        self.flows = [
            Flow(flow)
            for flow in sorted(
                settings.flows,
                key=lambda flow: (flow.operation == "update", priority[flow.table]),
            )
            if flow.enabled
        ]
        self.clock = clock
        self.utcnow = utcnow

    def step(self, stop: Event | None = None) -> int:
        now = self.clock()
        renewing = [flow for flow in self.flows if flow.starts_period(now)]
        counts = {
            flow.settings.table: self.generator.count(flow.settings.table)
            for flow in renewing
            if flow.settings.operation == "update"
        }
        for flow in renewing:
            flow.reset(now, counts.get(flow.settings.table, 0))
            logger.info(
                "period table=%s operation=%s limit=%s",
                flow.settings.table,
                flow.settings.operation,
                flow.budget,
            )
        completed = 0
        for flow in self.flows:
            if stop is not None and stop.is_set():
                break
            if any(item.starts_period(self.clock()) for item in self.flows):
                break
            if flow.completed >= flow.budget or self.clock() < flow.next_operation_at:
                continue
            entity_id = self.generator.execute(
                flow.settings.table,
                flow.settings.operation,
                self.utcnow(),
                excluded=tuple(flow.updated_ids),
            )
            flow.next_operation_at = self.clock() + flow.settings.interval_ms / 1000
            if entity_id is not None:
                flow.completed += 1
                completed += 1
                if flow.settings.operation == "update":
                    flow.updated_ids.add(entity_id)
                logger.info(
                    "committed table=%s operation=%s period_count=%s limit=%s",
                    flow.settings.table,
                    flow.settings.operation,
                    flow.completed,
                    flow.budget,
                )
        return completed

    def run(self, stop: Event) -> None:
        while not stop.is_set():
            self.step(stop)
            now = self.clock()
            deadlines = []
            for flow in self.flows:
                period_end = flow.period_start + flow.settings.period_ms / 1000
                deadlines.append(period_end)
                if flow.completed < flow.budget:
                    deadlines.append(flow.next_operation_at)
            stop.wait(max(0.001, min(deadlines, default=now + 1) - now))
