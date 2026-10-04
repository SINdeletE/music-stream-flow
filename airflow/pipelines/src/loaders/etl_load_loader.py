from src.models.dwh import EtlLoad

from .base import ClickHouseLoader


class EtlLoadLoader(ClickHouseLoader[EtlLoad]):
    model = EtlLoad
