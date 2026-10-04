from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimArtist


class DimArtistTransformer:
    @staticmethod
    async def transform(
        batch: list[DimArtist],
        client: AsyncClient,
    ) -> list[DimArtist]:
        artist_sks: dict[UUID, int | None] = {}
        for artist in batch:
            artist_sks[artist.artist_id] = None

        if not batch:
            return batch

        result = await client.query(
            query="""
                SELECT
                    artist_id,
                    argMax(artist_sk, (updated_at, artist_sk)) AS artist_sk
                FROM music_dwh.dim_artist_all
                WHERE artist_id IN {artist_ids:Array(UUID)}
                GROUP BY artist_id
            """,
            parameters={"artist_ids": list(artist_sks)},
        )

        for artist_id, artist_sk in result.result_rows:
            artist_sks[artist_id] = artist_sk

        for artist in batch:
            artist.artist_sk = artist_sks[artist.artist_id]

        return batch
