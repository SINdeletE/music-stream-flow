from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.models.dwh import DimTrack


class DimTrackTransformer:
    @staticmethod
    async def transform(
        batch: list[DimTrack],
        client: AsyncClient,
    ) -> list[DimTrack]:
        track_sks: dict[UUID, int | None] = {}
        for track in batch:
            track_sks[track.track_id] = None

        if not batch:
            return batch

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

        for track in batch:
            track.track_sk = track_sks[track.track_id]

        return batch
