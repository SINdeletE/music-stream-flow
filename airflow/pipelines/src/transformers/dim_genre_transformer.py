from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.genre_mapper import GenreMapper
from src.models.dwh import DimGenre
from src.models.oltp import Genre


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

        genre_sks: dict[UUID, int | None] = {}
        for entity in batch:
            genre_sks[entity.id] = None

        result = await client.query(
            query="""
                SELECT
                    genre_id,
                    argMax(genre_sk, (updated_at, genre_sk)) AS genre_sk
                FROM music_dwh.dim_genre
                WHERE genre_id IN {genre_ids:Array(UUID)}
                GROUP BY genre_id
            """,
            parameters={"genre_ids": list(genre_sks)},
        )

        for genre_id, genre_sk in result.result_rows:
            genre_sks[genre_id] = genre_sk

        return [
            GenreMapper.to_dwh(
                entity,
                load_id,
                genre_sk=genre_sks[entity.id],
            )
            for entity in batch
        ]
