from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.track_mapper import TrackMapper
from src.models.dwh import DimTrack
from src.models.oltp import Track

from ._dimension_lookup import lookup_dimension_sks


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

        track_sks = await lookup_dimension_sks(
            client,
            (entity.id for entity in batch),
            table_name="music_dwh.dim_track_all",
            entity_name="track",
        )

        return [
            TrackMapper.to_dwh(
                entity,
                load_id,
                track_sk=track_sks[entity.id],
            )
            for entity in batch
        ]
