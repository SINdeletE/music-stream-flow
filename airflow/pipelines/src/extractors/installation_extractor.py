from src.models.oltp import Installation

from .base import OltpExtractor


class InstallationExtractor(OltpExtractor[Installation]):
    model = Installation
    timestamp_column = Installation.updated_at
    id_columns = (Installation.id,)
