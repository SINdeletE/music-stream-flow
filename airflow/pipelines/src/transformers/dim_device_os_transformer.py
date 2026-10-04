from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimDeviceOs


class DimDeviceOsTransformer:
    @staticmethod
    async def transform(
        batch: list[DimDeviceOs],
        client: AsyncClient,
    ) -> list[DimDeviceOs]:
        device_os_sks: dict[UUID, int | None] = {}
        for device_os in batch:
            device_os_sks[device_os.device_os_id] = None

        if not batch:
            return batch

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

        for device_os in batch:
            device_os.device_os_sk = device_os_sks[device_os.device_os_id]

        return batch
