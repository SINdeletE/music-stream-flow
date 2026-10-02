from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DeviceOsHistory(Base):
    __tablename__ = "device_os_history"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    device_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("music_oltp.device.id", ondelete="CASCADE")
    )
    os_name: Mapped[str] = mapped_column(String(32))
    os_version: Mapped[str] = mapped_column(String(32))
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
