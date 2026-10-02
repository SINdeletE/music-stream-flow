from src.models.dwh import DimAppRelease
from src.models.oltp import AppRelease
from datetime import datetime
from uuid import UUID

class AppReleaseMapper:
    @staticmethod
    def to_dwh_app_release(entity: AppRelease, load_id: UUID) -> DimAppRelease:
        return DimAppRelease(app_release_id=entity.id, platform=entity.platform, 
                             app_release_version=entity.app_release_version, build=entity.build,
                             released_at=entity.released_at, load_id=load_id, source_created_at=entity.created_at)