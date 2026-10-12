from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.device_os_mapper import DeviceOsMapper
from src.models.dwh import DimDeviceOs
from src.models.oltp import DeviceOsHistory

from ._dimension_lookup import lookup_dimension_sks


class DimDeviceOsTransformer:
    @staticmethod
    async def transform(
        batch: list[DeviceOsHistory],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimDeviceOs]:
        if not batch:
            return []

        device_os_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_device_os_all",
            entity_name="device_os",
            version_column="valid_from",
        )

        return [
            DeviceOsMapper.to_dwh(
                entity,
                load_id,
                device_os_sk=device_os_sks[entity.id],
            )
            for entity in batch
        ]
