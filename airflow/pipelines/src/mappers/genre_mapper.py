from src.models.dwh import DimGenre
from src.models.oltp import Genre
from datetime import datetime
from uuid import UUID

class GenreMapper:
    @staticmethod
    def to_dwh_genre(entity: Genre, load_id: UUID) -> DimGenre:
        return DimGenre(genre_id=entity.id, name=entity.name,
                        load_id=load_id, source_created_at=entity.created_at,
                        updated_at=entity.updated_at, deleted_at=entity.deleted_at)