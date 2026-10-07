from src.models.oltp import Installation

from .oltp_extractor import OltpExtractor


class InstallationExtractor(OltpExtractor[Installation]):
    model = Installation
    timestamp_column = Installation.updated_at
    id_columns = (Installation.id,)
