from uuid import UUID

from src.models.dwh import DimAppRelease
from src.models.oltp import AppRelease


class AppReleaseMapper:
    @staticmethod
    def to_dwh(
        entity: AppRelease,
        load_id: UUID,
        *,
        app_release_sk: int | None = None,
    ) -> DimAppRelease:
        return DimAppRelease(
            app_release_sk=app_release_sk,
            app_release_id=entity.id,
            platform=entity.platform,
            app_release_version=entity.app_release_version,
            build=entity.build,
            released_at=entity.released_at,
            load_id=load_id,
            source_created_at=entity.created_at,
        )
