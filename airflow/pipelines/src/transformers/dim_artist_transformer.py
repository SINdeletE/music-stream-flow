from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.artist_mapper import ArtistMapper
from src.models.dwh import DimArtist
from src.models.oltp import Artist


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

        artist_sks: dict[UUID, int | None] = {}
        for entity in batch:
            artist_sks[entity.id] = None

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

        return [
            ArtistMapper.to_dwh(
                entity,
                load_id,
                artist_sk=artist_sks[entity.id],
            )
            for entity in batch
        ]
