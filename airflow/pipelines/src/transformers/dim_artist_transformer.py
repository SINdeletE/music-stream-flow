from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.artist_mapper import ArtistMapper
from src.models.dwh import DimArtist
from src.models.oltp import Artist

from ._dimension_lookup import lookup_dimension_sks


class DimArtistTransformer:
    @staticmethod
    async def transform(
        batch: list[Artist],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimArtist]:
        if not batch:
            return []

        artist_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_artist_all",
            entity_name="artist",
        )

        return [
            ArtistMapper.to_dwh(
                entity,
                load_id,
                artist_sk=artist_sks[entity.id],
            )
            for entity in batch
        ]
