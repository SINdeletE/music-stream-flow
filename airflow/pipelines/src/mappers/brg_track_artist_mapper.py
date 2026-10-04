from uuid import UUID

from src.models.dwh import BrgTrackArtist
from src.models.oltp import TrackArtist


class BrgTrackArtistMapper:
    @staticmethod
    def to_dwh(
        entity: TrackArtist,
        load_id: UUID,
        *,
        track_sk: int,
        artist_sk: int,
    ) -> BrgTrackArtist:
        if track_sk is None:
            raise ValueError("track_sk must be resolved before mapping a bridge")
        if artist_sk is None:
            raise ValueError("artist_sk must be resolved before mapping a bridge")

        return BrgTrackArtist(
            track_sk=track_sk,
            artist_sk=artist_sk,
            artist_number=entity.artist_number,
            load_id=load_id,
        )
