from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers import BrgTrackArtistMapper
from src.models.dwh import BrgTrackArtist, DimTrack, DimArtist
from src.models.oltp import TrackArtist

from ._dimension_lookup import resolve_dimension_sks


class BrgTrackArtistTransformer:
    @staticmethod
    async def transform(
        batch: list[TrackArtist],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[BrgTrackArtist]:
        if not batch:
            return []

        track_sks = await resolve_dimension_sks(
            client,
            (entity.track_id for entity in batch),
            table_name=DimTrack.table_to_insert(),
            entity_name="track",
        )
        artist_sks = await resolve_dimension_sks(
            client,
            (entity.artist_id for entity in batch),
            table_name=DimArtist.table_to_insert(),
            entity_name="artist",
        )

        return [
            BrgTrackArtistMapper.to_dwh(
                entity,
                load_id,
                track_sk=track_sks[entity.track_id],
                artist_sk=artist_sks[entity.artist_id],
            )
            for entity in batch
        ]
