from src.models.oltp import AppRelease

from .oltp_extractor import OltpExtractor


class AppReleaseExtractor(OltpExtractor[AppRelease]):
    model = AppRelease
    timestamp_column = AppRelease.created_at
    id_columns = (AppRelease.id,)
