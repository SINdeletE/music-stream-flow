from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.device_mapper import DeviceMapper
from src.models.dwh import DimDevice
from src.models.oltp import Device

from ._dimension_lookup import lookup_dimension_sks


class DimDeviceTransformer:
    @staticmethod
    async def transform(
        batch: list[Device],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimDevice]:
        if not batch:
            return []

        device_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_device_all",
            entity_name="device",
        )

        return [
            DeviceMapper.to_dwh(
                entity,
                load_id,
                device_sk=device_sks[entity.id],
            )
            for entity in batch
        ]
