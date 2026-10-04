from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimGenre


class DimGenreTransformer:
    @staticmethod
    async def transform(
        batch: list[DimGenre],
        client: AsyncClient,
    ) -> list[DimGenre]:
        genre_sks: dict[UUID, int | None] = {}
        for genre in batch:
            genre_sks[genre.genre_id] = None

        if not batch:
            return batch

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

        for genre in batch:
            genre.genre_sk = genre_sks[genre.genre_id]

        return batch
