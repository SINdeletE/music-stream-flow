from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.release_mapper import ReleaseMapper
from src.models.dwh import DimRelease
from src.models.oltp import Release


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

        release_sks: dict[UUID, int | None] = {}
        for entity in batch:
            release_sks[entity.id] = None

        result = await client.query(
            query="""
                SELECT
                    release_id,
                    argMax(release_sk, (updated_at, release_sk)) AS release_sk
                FROM music_dwh.dim_release_all
                WHERE release_id IN {release_ids:Array(UUID)}
                GROUP BY release_id
            """,
            parameters={"release_ids": list(release_sks)},
        )

        for release_id, release_sk in result.result_rows:
            release_sks[release_id] = release_sk

        return [
            ReleaseMapper.to_dwh(
                entity,
                load_id,
                release_sk=release_sks[entity.id],
            )
            for entity in batch
        ]
