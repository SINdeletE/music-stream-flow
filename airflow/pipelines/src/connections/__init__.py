"""Database settings for pipeline connections."""

from .dwh_settings import DwhSettings
from .oltp_settings import OltpSettings

__all__ = ["DwhSettings", "OltpSettings"]
