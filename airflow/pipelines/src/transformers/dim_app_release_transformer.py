from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.app_release_mapper import AppReleaseMapper
from src.models.dwh import DimAppRelease
from src.models.oltp import AppRelease


class DimAppReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[AppRelease],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimAppRelease]:
        if not batch:
            return []

        app_release_sks: dict[UUID, int | None] = {}
        for entity in batch:
            app_release_sks[entity.id] = None

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

        return [
            AppReleaseMapper.to_dwh(
                entity,
                load_id,
                app_release_sk=app_release_sks[entity.id],
            )
            for entity in batch
        ]
