from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimDevice


class DimDeviceTransformer:
    @staticmethod
    async def transform(
        batch: list[DimDevice],
        client: AsyncClient,
    ) -> list[DimDevice]:
        device_sks: dict[UUID, int | None] = {}
        for device in batch:
            device_sks[device.device_id] = None

        if not batch:
            return batch

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

        for device in batch:
            device.device_sk = device_sks[device.device_id]

        return batch
