from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers import BrgTrackReleaseMapper
from src.models.dwh import BrgTrackRelease, DimTrack, DimRelease
from src.models.oltp import TrackRelease

from ._dimension_lookup import resolve_dimension_sks


class BrgTrackReleaseTransformer:
    @staticmethod
    async def transform(
        batch: list[TrackRelease],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[BrgTrackRelease]:
        if not batch:
            return []

        track_sks = await resolve_dimension_sks(
            client,
            (entity.track_id for entity in batch),
            table_name=DimTrack.table_to_insert(),
            entity_name="track",
        )
        release_sks = await resolve_dimension_sks(
            client,
            (entity.release_id for entity in batch),
            table_name=DimRelease.table_to_insert(),
            entity_name="release",
        )

        return [
            BrgTrackReleaseMapper.to_dwh(
                entity,
                load_id,
                track_sk=track_sks[entity.track_id],
                release_sk=release_sks[entity.release_id],
            )
            for entity in batch
        ]
