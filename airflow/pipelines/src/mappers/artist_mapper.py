from src.models.dwh import DimArtist
from src.models.oltp import Artist
from datetime import datetime
from uuid import UUID

class TrackMapper:
    @staticmethod
    def to_dwh_artist(entity: Artist, load_id: UUID) -> DimArtist:
        return DimArtist(artist_id=entity.id, nickname=entity.nickname,
                         name=entity.name, middle_name=entity.middle_name,
                         last_name=entity.last_name, country_code=entity.country_code,
                         biography=entity.biography, is_verified=entity.is_verified,
                         load_id=load_id, source_created_at=entity.created_at,
                         updated_at=entity.updated_at, deleted_at=entity.deleted_at)
