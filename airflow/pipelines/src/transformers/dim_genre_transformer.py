from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.genre_mapper import GenreMapper
from src.models.dwh import DimGenre
from src.models.oltp import Genre

from ._dimension_lookup import lookup_dimension_sks


class DimGenreTransformer:
    @staticmethod
    async def transform(
        batch: list[Genre],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimGenre]:
        if not batch:
            return []

        genre_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_genre",
            entity_name="genre",
        )

        return [
            GenreMapper.to_dwh(
                entity,
                load_id,
                genre_sk=genre_sks[entity.id],
            )
            for entity in batch
        ]
