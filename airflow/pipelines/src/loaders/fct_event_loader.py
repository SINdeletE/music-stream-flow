from src.models.dwh import FctEvent

from .base import ClickHouseLoader


class FctEventLoader(ClickHouseLoader[FctEvent]):
    model = FctEvent
    generated_columns = ("event_sk",)
