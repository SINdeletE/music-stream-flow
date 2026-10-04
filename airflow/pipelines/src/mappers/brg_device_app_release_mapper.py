from uuid import UUID

from src.models.dwh import BrgDeviceAppRelease
from src.models.oltp import Installation


class BrgDeviceAppReleaseMapper:
    @staticmethod
    def to_dwh(
        entity: Installation,
        load_id: UUID,
        *,
        device_sk: int,
        app_release_sk: int,
    ) -> BrgDeviceAppRelease:
        if device_sk is None:
            raise ValueError("device_sk must be resolved before mapping a bridge")
        if app_release_sk is None:
            raise ValueError("app_release_sk must be resolved before mapping a bridge")

        return BrgDeviceAppRelease(
            device_sk=device_sk,
            app_release_sk=app_release_sk,
            load_id=load_id,
        )
