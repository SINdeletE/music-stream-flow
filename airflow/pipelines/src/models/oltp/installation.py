from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Installation(Base):
    __tablename__ = "installation"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    device_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("music_oltp.device.id", ondelete="RESTRICT")
    )
    app_release_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.app_release.id", ondelete="RESTRICT"),
    )
    installed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
