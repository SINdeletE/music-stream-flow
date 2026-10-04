from src.models.dwh import DimEventType

from .base import ClickHouseLoader


class DimEventTypeLoader(ClickHouseLoader[DimEventType]):
    model = DimEventType
    generated_columns = ("event_type_sk",)
