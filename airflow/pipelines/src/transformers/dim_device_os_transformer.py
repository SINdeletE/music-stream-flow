from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.device_os_mapper import DeviceOsMapper
from src.models.dwh import DimDeviceOs
from src.models.oltp import DeviceOsHistory


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

        device_os_sks: dict[UUID, int | None] = {}
        for entity in batch:
            device_os_sks[entity.id] = None

        result = await client.query(
            query="""
                SELECT
                    device_os_id,
                    argMax(device_os_sk, (valid_from, device_os_sk)) AS device_os_sk
                FROM music_dwh.dim_device_os_all
                WHERE device_os_id IN {device_os_ids:Array(UUID)}
                GROUP BY device_os_id
            """,
            parameters={"device_os_ids": list(device_os_sks)},
        )

        for device_os_id, device_os_sk in result.result_rows:
            device_os_sks[device_os_id] = device_os_sk

        return [
            DeviceOsMapper.to_dwh(
                entity,
                load_id,
                device_os_sk=device_os_sks[entity.id],
            )
            for entity in batch
        ]
