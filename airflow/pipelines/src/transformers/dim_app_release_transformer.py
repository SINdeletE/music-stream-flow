from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimAppRelease


class DimAppReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[DimAppRelease],
        client: AsyncClient,
    ) -> list[DimAppRelease]:
        app_release_sks: dict[UUID, int | None] = {}
        for app_release in batch:
            app_release_sks[app_release.app_release_id] = None

        if not batch:
            return batch

        result = await client.query(
            query="""
                SELECT
                    app_release_id,
                    argMax(app_release_sk, (source_created_at, app_release_sk)) AS app_release_sk
                FROM music_dwh.dim_app_release
                WHERE app_release_id IN {app_release_ids:Array(UUID)}
                GROUP BY app_release_id
            """,
            parameters={"app_release_ids": list(app_release_sks)},
        )

        for app_release_id, app_release_sk in result.result_rows:
            app_release_sks[app_release_id] = app_release_sk

        for app_release in batch:
            app_release.app_release_sk = app_release_sks[app_release.app_release_id]

        return batch
