from uuid import UUID

from src.models.dwh import DimTrack
from src.models.oltp import Track


class TrackMapper:
    @staticmethod
    def to_dwh(
        entity: Track,
        load_id: UUID,
        *,
        track_sk: int | None = None,
    ) -> DimTrack:
        return DimTrack(
            track_sk=track_sk,
            track_id=entity.id,
            name=entity.name,
            duration_ms=entity.duration_ms,
            is_available=entity.is_available,
            load_id=load_id,
            source_created_at=entity.created_at,
            updated_at=entity.updated_at,
            deleted_at=entity.deleted_at,
        )
