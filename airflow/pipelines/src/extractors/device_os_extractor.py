from src.models.oltp import DeviceOsHistory

from .oltp_extractor import OltpExtractor


class DeviceOsExtractor(OltpExtractor[DeviceOsHistory]):
    model = DeviceOsHistory
    timestamp_column = DeviceOsHistory.created_at
    id_columns = (DeviceOsHistory.id,)
