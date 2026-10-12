from uuid import UUID

from src.models.dwh import DimDeviceOs
from src.models.oltp import DeviceOsHistory


class DeviceOsMapper:
    @staticmethod
    def to_dwh(
        entity: DeviceOsHistory,
        load_id: UUID,
        *,
        device_os_sk: int | None = None,
    ) -> DimDeviceOs:
        return DimDeviceOs(
            device_os_sk=device_os_sk,
            device_os_id=entity.id,
            name=entity.os_name,
            device_os_version=entity.os_version,
            valid_from=entity.valid_from,
            load_id=load_id,
            source_created_at=entity.created_at,
        )
