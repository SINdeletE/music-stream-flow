from src.models.oltp import Device

from .base import OltpExtractor


class DeviceExtractor(OltpExtractor[Device]):
    model = Device
    timestamp_column = Device.updated_at
    id_columns = (Device.id,)
