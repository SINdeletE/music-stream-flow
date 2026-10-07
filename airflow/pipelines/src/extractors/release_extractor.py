from src.models.oltp import Release

from .oltp_extractor import OltpExtractor


class ReleaseExtractor(OltpExtractor[Release]):
    model = Release
    timestamp_column = Release.updated_at
    id_columns = (Release.id,)
