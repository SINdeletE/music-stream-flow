from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.app_release_mapper import AppReleaseMapper
from src.models.dwh import DimAppRelease
from src.models.oltp import AppRelease

from ._dimension_lookup import lookup_dimension_sks


class DimAppReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[AppRelease],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimAppRelease]:
        if not batch:
            return []

        app_release_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_app_release",
            entity_name="app_release",
            version_column="source_created_at",
        )

        return [
            AppReleaseMapper.to_dwh(
                entity,
                load_id,
                app_release_sk=app_release_sks[entity.id],
            )
            for entity in batch
        ]
