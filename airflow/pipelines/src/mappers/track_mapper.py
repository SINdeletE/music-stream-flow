from src.models.dwh import DimTrack
from src.models.oltp import Track
from datetime import datetime
from uuid import UUID

class TrackMapper:
    @staticmethod
    def to_dwh_track(entity: Track, load_id: UUID) -> DimTrack:
        return DimTrack(track_id=entity.id, name=entity.name, duration_ms=entity.duration_ms, 
                        is_available=entity.is_available, load_id=load_id,
                        source_created_at=entity.created_at, updated_at=entity.updated_at,
                        deleted_at=entity.deleted_at)