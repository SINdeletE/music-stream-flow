from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers import BrgTrackGenreMapper
from src.models.dwh import BrgTrackGenre, DimGenre, DimTrack
from src.models.oltp import TrackGenre

from ._dimension_lookup import resolve_dimension_sks


class BrgTrackGenreTransformer:
    @staticmethod
    async def transform(
        batch: list[TrackGenre],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[BrgTrackGenre]:
        if not batch:
            return []

        track_sks = await resolve_dimension_sks(
            client,
            (entity.track_id for entity in batch),
            table_name=DimTrack.table_to_insert(),
            entity_name="track",
        )
        genre_sks = await resolve_dimension_sks(
            client,
            (entity.genre_id for entity in batch),
            table_name=DimGenre.table_to_insert(),
            entity_name="genre",
        )

        return [
            BrgTrackGenreMapper.to_dwh(
                entity,
                load_id,
                track_sk=track_sks[entity.track_id],
                genre_sk=genre_sks[entity.genre_id],
            )
            for entity in batch
        ]
