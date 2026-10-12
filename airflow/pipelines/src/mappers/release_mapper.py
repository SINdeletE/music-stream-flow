from uuid import UUID

from src.models.dwh import DimRelease
from src.models.oltp import Release


class ReleaseMapper:
    @staticmethod
    def to_dwh(
        entity: Release,
        load_id: UUID,
        *,
        release_sk: int | None = None,
    ) -> DimRelease:
        return DimRelease(
            release_sk=release_sk,
            release_id=entity.id,
            name=entity.name,
            is_available=entity.is_available,
            released_at=entity.released_at,
            load_id=load_id,
            source_created_at=entity.created_at,
            updated_at=entity.updated_at,
            deleted_at=entity.deleted_at,
        )
