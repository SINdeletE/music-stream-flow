from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers import BrgArtistReleaseMapper
from src.models.dwh import BrgArtistRelease, DimArtist, DimRelease
from src.models.oltp import ArtistRelease

from ._dimension_lookup import resolve_dimension_sks


class BrgArtistReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[ArtistRelease],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[BrgArtistRelease]:
        if not batch:
            return []

        artist_sks = await resolve_dimension_sks(
            client,
            (entity.artist_id for entity in batch),
            table_name=DimArtist.table_to_insert(),
            entity_name="artist",
        )
        release_sks = await resolve_dimension_sks(
            client,
            (entity.release_id for entity in batch),
            table_name=DimRelease.table_to_insert(),
            entity_name="release",
        )

        return [
            BrgArtistReleaseMapper.to_dwh(
                entity,
                load_id,
                artist_sk=artist_sks[entity.artist_id],
                release_sk=release_sks[entity.release_id],
            )
            for entity in batch
        ]
