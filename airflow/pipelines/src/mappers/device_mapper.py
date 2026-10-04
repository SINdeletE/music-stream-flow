from src.models.dwh import DimDevice
from src.models.oltp import Device
from datetime import datetime
from uuid import UUID

class DeviceMapper:
    @staticmethod
    def to_dwh(
        entity: Device,
        load_id: UUID,
        *,
        device_sk: int | None = None,
    ) -> DimDevice:
        return DimDevice(device_sk=device_sk, device_id=entity.id, user_id=entity.user_id,
                         name=entity.name, device_version=entity.device_version,
                         load_id=load_id, source_created_at=entity.created_at, updated_at=entity.updated_at)
