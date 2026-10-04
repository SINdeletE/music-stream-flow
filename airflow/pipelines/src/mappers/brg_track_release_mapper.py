from uuid import UUID

from src.models.dwh import BrgTrackRelease
from src.models.oltp import TrackRelease


class BrgTrackReleaseMapper:
    @staticmethod
    def to_dwh(
        entity: TrackRelease,
        load_id: UUID,
        *,
        track_sk: int,
        release_sk: int,
    ) -> BrgTrackRelease:
        if track_sk is None:
            raise ValueError("track_sk must be resolved before mapping a bridge")
        if release_sk is None:
            raise ValueError("release_sk must be resolved before mapping a bridge")

        return BrgTrackRelease(
            track_sk=track_sk,
            release_sk=release_sk,
            track_number=entity.track_number,
            disc_number=entity.disc_number,
            load_id=load_id,
        )
