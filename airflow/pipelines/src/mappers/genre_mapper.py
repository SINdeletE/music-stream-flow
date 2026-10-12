from uuid import UUID

from src.models.dwh import DimGenre
from src.models.oltp import Genre


class GenreMapper:
    @staticmethod
    def to_dwh(
        entity: Genre,
        load_id: UUID,
        *,
        genre_sk: int | None = None,
    ) -> DimGenre:
        return DimGenre(
            genre_sk=genre_sk,
            genre_id=entity.id,
            name=entity.name,
            load_id=load_id,
            source_created_at=entity.created_at,
            updated_at=entity.updated_at,
            deleted_at=entity.deleted_at,
        )
