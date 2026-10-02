from src.models.dwh import DimDeviceOs
from src.models.oltp import DeviceOsHistory
from datetime import datetime
from uuid import UUID

class DeviceOsMapper:
    @staticmethod
    def to_dwh_device_os(entity: DeviceOsHistory, load_id: UUID) -> DimDeviceOs:
        return DimDeviceOs(device_os_id=entity.id, name=entity.os_name, device_os_version=entity.os_version,
                           valid_from=entity.valid_from, load_id=load_id, source_created_at=entity.created_at)