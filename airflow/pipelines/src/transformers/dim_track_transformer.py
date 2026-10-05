from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.track_mapper import TrackMapper
from src.models.dwh import DimTrack
from src.models.oltp import Track


class DimTrackTransformer:
    @staticmethod
    async def transform(
        batch: list[Track],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[DimTrack]:
        if not batch:
            return []

        track_sks: dict[UUID, int | None] = {}
        for entity in batch:
            track_sks[entity.id] = None

        result = await client.query(
            query="""
                SELECT
                    track_id,
                    argMax(track_sk, (updated_at, track_sk)) AS track_sk
                FROM music_dwh.dim_track_all
                WHERE track_id IN {track_ids:Array(UUID)}
                GROUP BY track_id
            """,
            parameters={"track_ids": list(track_sks)},
        )

        for track_id, track_sk in result.result_rows:
            track_sks[track_id] = track_sk

        return [
            TrackMapper.to_dwh(
                entity,
                load_id,
                track_sk=track_sks[entity.id],
            )
            for entity in batch
        ]
