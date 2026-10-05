from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.device_mapper import DeviceMapper
from src.models.dwh import DimDevice
from src.models.oltp import Device


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

        device_sks: dict[UUID, int | None] = {}
        for entity in batch:
            device_sks[entity.id] = None

        result = await client.query(
            query="""
                SELECT
                    device_id,
                    argMax(device_sk, (updated_at, device_sk)) AS device_sk
                FROM music_dwh.dim_device_all
                WHERE device_id IN {device_ids:Array(UUID)}
                GROUP BY device_id
            """,
            parameters={"device_ids": list(device_sks)},
        )

        for device_id, device_sk in result.result_rows:
            device_sks[device_id] = device_sk

        return [
            DeviceMapper.to_dwh(
                entity,
                load_id,
                device_sk=device_sks[entity.id],
            )
            for entity in batch
        ]
