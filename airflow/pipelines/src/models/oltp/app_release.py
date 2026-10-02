from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class AppRelease(Base):
    __tablename__ = "app_release"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    platform: Mapped[str] = mapped_column(String(16))
    app_release_version: Mapped[str] = mapped_column(String(16))
    build: Mapped[str] = mapped_column(String(32))
    released_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
