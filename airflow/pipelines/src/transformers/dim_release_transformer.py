from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimRelease


class DimReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[DimRelease],
        client: AsyncClient,
    ) -> list[DimRelease]:
        release_sks: dict[UUID, int | None] = {}
        for release in batch:
            release_sks[release.release_id] = None

        if not batch:
            return batch

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

        for release in batch:
            release.release_sk = release_sks[release.release_id]

        return batch
