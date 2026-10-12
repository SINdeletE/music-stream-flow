from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers import BrgDeviceAppReleaseMapper
from src.models.dwh import BrgDeviceAppRelease, DimAppRelease, DimDevice
from src.models.oltp import Installation

from ._dimension_lookup import resolve_dimension_sks


class BrgDeviceAppReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[Installation],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[BrgDeviceAppRelease]:
        if not batch:
            return []

        device_sks = await resolve_dimension_sks(
            client,
            (entity.device_id for entity in batch),
            table_name=DimDevice.table_to_insert(),
            entity_name="device",
        )
        app_release_sks = await resolve_dimension_sks(
            client,
            (entity.app_release_id for entity in batch),
            table_name=DimAppRelease.table_to_insert(),
            entity_name="app_release",
            version_column="source_created_at",
        )

        return [
            BrgDeviceAppReleaseMapper.to_dwh(
                entity,
                load_id,
                device_sk=device_sks[entity.device_id],
                app_release_sk=app_release_sks[entity.app_release_id],
            )
            for entity in batch
        ]
