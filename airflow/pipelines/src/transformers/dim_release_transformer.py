from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.release_mapper import ReleaseMapper
from src.models.dwh import DimRelease
from src.models.oltp import Release

from ._dimension_lookup import lookup_dimension_sks


class DimReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[Release],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimRelease]:
        if not batch:
            return []

        release_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_release_all",
            entity_name="release",
        )

        return [
            ReleaseMapper.to_dwh(
                entity,
                load_id,
                release_sk=release_sks[entity.id],
            )
            for entity in batch
        ]
