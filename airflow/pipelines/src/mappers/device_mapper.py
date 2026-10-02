from src.models.dwh import DimDevice
from src.models.oltp import Device
from datetime import datetime
from uuid import UUID

class DeviceMapper:
    @staticmethod
    def to_dwh_device(entity: Device, load_id: UUID) -> DimDevice:
        return DimDevice(device_id=entity.id, user_id=entity.user_id,
                         name=entity.name, device_version=entity.device_version,
                         load_id=load_id, updated_at=entity.updated_at, deleted_at=entity.d)