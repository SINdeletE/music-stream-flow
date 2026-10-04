from uuid import UUID

from src.models.dwh import BrgTrackGenre
from src.models.oltp import TrackGenre


class BrgTrackGenreMapper:
    @staticmethod
    def to_dwh(
        entity: TrackGenre,
        load_id: UUID,
        *,
        track_sk: int,
        genre_sk: int,
    ) -> BrgTrackGenre:
        if track_sk is None:
            raise ValueError("track_sk must be resolved before mapping a bridge")
        if genre_sk is None:
            raise ValueError("genre_sk must be resolved before mapping a bridge")

        return BrgTrackGenre(
            track_sk=track_sk,
            genre_sk=genre_sk,
            load_id=load_id,
        )
